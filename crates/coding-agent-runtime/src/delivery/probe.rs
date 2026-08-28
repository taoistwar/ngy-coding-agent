use std::ffi::OsString;
#[cfg(feature = "test-support")]
use std::path::Path;
#[cfg(windows)]
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::command_policy::{
    CommandPolicyError, DeliveryGitEmptyConfig, DeliveryGitProbeCommands,
    DeliveryGitRepositoryProbeCommands, ExecutionDirectory, PinnedExecutable, ProbeGitObjectId,
    ValidatedCommand,
};
use crate::process_liveness::ProcessLivenessScope;
use crate::process_supervisor::{
    ChildEnvironment, CommandResult, PlatformEnvironment, ProcessError, ProcessLimits,
    ProcessSupervisor,
};

use super::{
    DeliveryGitObjectFormat, DeliveryGitProbeError, DeliveryGitVersion, ProbedDeliveryGit,
};

const MINIMUM_GIT_MAJOR: u32 = 2;
const MINIMUM_GIT_MINOR: u32 = 45;

mod workspace;

use workspace::ProbeWorkspace;

pub async fn probe_delivery_git(
    git: Arc<PinnedExecutable>,
    private_runtime: Arc<ExecutionDirectory>,
    process_scope: ProcessLivenessScope,
    process_limits: ProcessLimits,
    timeout: Duration,
    cancellation: CancellationToken,
) -> Result<ProbedDeliveryGit, DeliveryGitProbeError> {
    probe_delivery_git_with_hooks(
        git,
        private_runtime,
        process_scope,
        process_limits,
        timeout,
        cancellation,
        ProbeExecutionHooks::none(),
    )
    .await
}

/// Test-only full probe entry point with one callback immediately after the
/// private repository is initialized.
///
/// The callback exists solely to inject a repository-local adversarial fixture
/// before the real capability merge. Production callers cannot opt into it.
#[cfg(feature = "test-support")]
#[doc(hidden)]
pub async fn probe_delivery_git_with_after_initialize_hook_for_test(
    git: Arc<PinnedExecutable>,
    private_runtime: Arc<ExecutionDirectory>,
    process_scope: ProcessLivenessScope,
    process_limits: ProcessLimits,
    timeout: Duration,
    cancellation: CancellationToken,
    after_initialize: impl Fn(&Path) + Send + Sync + 'static,
) -> Result<ProbedDeliveryGit, DeliveryGitProbeError> {
    probe_delivery_git_with_hooks(
        git,
        private_runtime,
        process_scope,
        process_limits,
        timeout,
        cancellation,
        ProbeExecutionHooks::after_initialize(after_initialize),
    )
    .await
}

async fn probe_delivery_git_with_hooks(
    git: Arc<PinnedExecutable>,
    private_runtime: Arc<ExecutionDirectory>,
    process_scope: ProcessLivenessScope,
    process_limits: ProcessLimits,
    timeout: Duration,
    cancellation: CancellationToken,
    hooks: ProbeExecutionHooks,
) -> Result<ProbedDeliveryGit, DeliveryGitProbeError> {
    git.revalidate()
        .map_err(|_| DeliveryGitProbeError::ExecutableChanged)?;
    private_runtime
        .revalidate()
        .map_err(|_| DeliveryGitProbeError::InvalidConfiguration)?;
    if timeout.is_zero() || timeout > process_limits.max_command_timeout() {
        return Err(DeliveryGitProbeError::InvalidConfiguration);
    }

    let workspace = ProbeWorkspace::create(Arc::clone(&private_runtime))?;
    let repository = workspace.directory();
    let empty_config = match workspace.git_sandbox() {
        Ok(sandbox) => sandbox,
        Err(error) => {
            drop(repository);
            workspace.cleanup()?;
            return Err(error);
        }
    };
    let environment = match probe_environment(&repository, &empty_config) {
        Ok(environment) => environment,
        Err(error) => {
            drop(empty_config);
            drop(repository);
            workspace.cleanup()?;
            return Err(error);
        }
    };
    let supervisor = ProcessSupervisor::new(process_limits, process_scope);
    let probe_result = run_capability_probe(CapabilityProbeInput {
        git: Arc::clone(&git),
        repository,
        empty_config,
        environment,
        timeout,
        runner: ProbeCommandRunner::new(&supervisor, &cancellation),
        hooks: &hooks,
    })
    .await;
    supervisor.shutdown().await;

    if matches!(&probe_result, Err(DeliveryGitProbeError::CleanupUnproven)) {
        return Err(DeliveryGitProbeError::CleanupUnproven);
    }
    finish_probe(git, private_runtime, probe_result, workspace.cleanup())
}

#[cfg(feature = "test-support")]
type AfterRepositoryInitializeHook = Box<dyn Fn(&Path) + Send + Sync + 'static>;

struct ProbeExecutionHooks {
    #[cfg(feature = "test-support")]
    after_initialize: Option<AfterRepositoryInitializeHook>,
}

impl ProbeExecutionHooks {
    const fn none() -> Self {
        Self {
            #[cfg(feature = "test-support")]
            after_initialize: None,
        }
    }

    #[cfg(feature = "test-support")]
    fn after_initialize(after_initialize: impl Fn(&Path) + Send + Sync + 'static) -> Self {
        Self {
            after_initialize: Some(Box::new(after_initialize)),
        }
    }

    fn after_repository_initialize(&self, repository: &ExecutionDirectory) {
        #[cfg(feature = "test-support")]
        if let Some(hook) = &self.after_initialize {
            hook(repository.path());
        }
        #[cfg(not(feature = "test-support"))]
        let _ = (self, repository);
    }
}

fn finish_probe(
    git: Arc<PinnedExecutable>,
    private_runtime: Arc<ExecutionDirectory>,
    probe_result: Result<ProbeFacts, DeliveryGitProbeError>,
    cleanup_result: Result<(), DeliveryGitProbeError>,
) -> Result<ProbedDeliveryGit, DeliveryGitProbeError> {
    cleanup_result?;
    let facts = probe_result?;
    let handle = ProbedDeliveryGit::from_successful_probe(
        git,
        private_runtime,
        facts.version,
        facts.object_format,
    )?;
    handle
        .mutation_command_factory()
        .and_then(|factory| factory.revalidate())
        .map_err(|_| DeliveryGitProbeError::ExecutableChanged)?;
    Ok(handle)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ProbeFacts {
    version: DeliveryGitVersion,
    object_format: DeliveryGitObjectFormat,
}

struct CapabilityProbeInput<'a> {
    git: Arc<PinnedExecutable>,
    repository: Arc<ExecutionDirectory>,
    empty_config: Arc<DeliveryGitEmptyConfig>,
    environment: ChildEnvironment,
    timeout: Duration,
    runner: ProbeCommandRunner<'a>,
    hooks: &'a ProbeExecutionHooks,
}

async fn run_capability_probe(
    input: CapabilityProbeInput<'_>,
) -> Result<ProbeFacts, DeliveryGitProbeError> {
    let CapabilityProbeInput {
        git,
        repository,
        empty_config,
        environment,
        timeout,
        runner,
        hooks,
    } = input;
    let commands = DeliveryGitProbeCommands::try_new(
        git,
        Arc::clone(&repository),
        empty_config,
        environment,
        timeout,
    )
    .map_err(map_command_policy_error)?;

    let version = parse_git_version(&runner.run(commands.version()?).await?)?;
    if !version.is_at_least(MINIMUM_GIT_MAJOR, MINIMUM_GIT_MINOR) {
        return Err(DeliveryGitProbeError::CapabilityUnavailable);
    }
    runner.run(commands.initialize_repository()?).await?;
    hooks.after_repository_initialize(&repository);

    let git_directory = Arc::new(
        ExecutionDirectory::open(repository.path().join(".git"))
            .map_err(|_| DeliveryGitProbeError::CapabilityUnavailable)?,
    );
    let repository_commands = commands
        .bind_repository(git_directory)
        .map_err(map_command_policy_error)?;
    let object_format =
        parse_object_format(&runner.run(repository_commands.object_format()?).await?)?;
    let oid_length = object_format.hexadecimal_length();
    let graph = create_probe_graph(&repository_commands, &runner, oid_length).await?;
    let merged_target =
        prove_merge_capabilities(&repository_commands, &runner, &graph, oid_length).await?;
    prove_atomic_ref_transaction(
        &repository_commands,
        &runner,
        &merged_target,
        &graph.source,
        oid_length,
    )
    .await?;

    Ok(ProbeFacts {
        version,
        object_format,
    })
}

struct ProbeGraph {
    source_tree: ProbeGitObjectId,
    target: ProbeGitObjectId,
    source: ProbeGitObjectId,
}

async fn create_probe_graph(
    commands: &DeliveryGitRepositoryProbeCommands,
    runner: &ProbeCommandRunner<'_>,
    oid_length: usize,
) -> Result<ProbeGraph, DeliveryGitProbeError> {
    let empty_tree = parse_object_id(&runner.run(commands.empty_tree()?).await?, oid_length)?;
    let blob = parse_object_id(&runner.run(commands.probe_blob()?).await?, oid_length)?;
    let source_tree =
        parse_object_id(&runner.run(commands.source_tree(&blob)?).await?, oid_length)?;
    let base = parse_object_id(
        &runner.run(commands.base_commit(&empty_tree)?).await?,
        oid_length,
    )?;
    let target = parse_object_id(
        &runner
            .run(commands.target_commit(&empty_tree, &base)?)
            .await?,
        oid_length,
    )?;
    let source = parse_object_id(
        &runner
            .run(commands.source_commit(&source_tree, &base)?)
            .await?,
        oid_length,
    )?;
    Ok(ProbeGraph {
        source_tree,
        target,
        source,
    })
}

async fn prove_merge_capabilities(
    commands: &DeliveryGitRepositoryProbeCommands,
    runner: &ProbeCommandRunner<'_>,
    graph: &ProbeGraph,
    oid_length: usize,
) -> Result<ProbeGitObjectId, DeliveryGitProbeError> {
    let merge_tree = runner
        .run(commands.merge_tree(&graph.target, &graph.source)?)
        .await?;
    require_clean_merge_tree(&merge_tree, &graph.source_tree, oid_length)?;
    runner.run(commands.set_target_ref(&graph.target)?).await?;
    runner.run(commands.set_source_ref(&graph.source)?).await?;
    runner.run(commands.merge(&graph.source)?).await?;
    parse_object_id(&runner.run(commands.resolve_head()?).await?, oid_length)
}

async fn prove_atomic_ref_transaction(
    commands: &DeliveryGitRepositoryProbeCommands,
    runner: &ProbeCommandRunner<'_>,
    merged_target: &ProbeGitObjectId,
    source: &ProbeGitObjectId,
    oid_length: usize,
) -> Result<(), DeliveryGitProbeError> {
    let transaction = runner
        .run(commands.delete_source_transaction(merged_target, source)?)
        .await?;
    if transaction.as_slice() != b"start: ok\nprepare: ok\ncommit: ok\n" {
        return Err(DeliveryGitProbeError::CapabilityUnavailable);
    }
    runner
        .run_expecting(commands.source_ref_exists()?, &[1])
        .await?;
    let target_after_transaction =
        parse_object_id(&runner.run(commands.resolve_head()?).await?, oid_length)?;
    if target_after_transaction != *merged_target {
        return Err(DeliveryGitProbeError::CapabilityUnavailable);
    }
    Ok(())
}

struct ProbeCommandRunner<'a> {
    supervisor: &'a ProcessSupervisor,
    cancellation: &'a CancellationToken,
}

impl<'a> ProbeCommandRunner<'a> {
    const fn new(supervisor: &'a ProcessSupervisor, cancellation: &'a CancellationToken) -> Self {
        Self {
            supervisor,
            cancellation,
        }
    }

    async fn run(&self, command: ValidatedCommand) -> Result<Vec<u8>, DeliveryGitProbeError> {
        self.run_expecting(command, &[0]).await
    }

    async fn run_expecting(
        &self,
        command: ValidatedCommand,
        expected_exit_codes: &[i32],
    ) -> Result<Vec<u8>, DeliveryGitProbeError> {
        let result = self
            .supervisor
            .run(command, self.cancellation.clone())
            .await
            .map_err(map_process_error)?;
        checked_stdout(result, expected_exit_codes)
    }
}

fn checked_stdout(
    result: CommandResult,
    expected_exit_codes: &[i32],
) -> Result<Vec<u8>, DeliveryGitProbeError> {
    if result.cancelled {
        return Err(DeliveryGitProbeError::Cancelled);
    }
    if result.timed_out
        || result.signal.is_some()
        || !expected_exit_codes
            .iter()
            .any(|expected| result.exit_code == Some(*expected))
        || result.truncated
        || result.stdout.truncated
        || result.stderr.truncated
        || !result.stdout.complete
        || !result.stderr.complete
    {
        return Err(DeliveryGitProbeError::CapabilityUnavailable);
    }
    let mut stderr = result.stderr.head;
    stderr.extend(result.stderr.tail);
    if !stderr.is_empty() {
        return Err(DeliveryGitProbeError::CapabilityUnavailable);
    }
    let mut stdout = result.stdout.head;
    stdout.extend(result.stdout.tail);
    Ok(stdout)
}

fn map_process_error(error: ProcessError) -> DeliveryGitProbeError {
    if error.process_cleanup_is_unproven() {
        DeliveryGitProbeError::CleanupUnproven
    } else if matches!(
        error,
        ProcessError::CommandPolicy(CommandPolicyError::IdentityChanged)
    ) {
        DeliveryGitProbeError::ExecutableChanged
    } else {
        DeliveryGitProbeError::CapabilityUnavailable
    }
}

fn map_command_policy_error(error: crate::CommandPolicyError) -> DeliveryGitProbeError {
    error.into()
}

fn parse_git_version(output: &[u8]) -> Result<DeliveryGitVersion, DeliveryGitProbeError> {
    let line = parse_one_line(output)?;
    let value = line
        .strip_prefix("git version ")
        .ok_or(DeliveryGitProbeError::CapabilityUnavailable)?;
    let mut components = value.split('.');
    let major = parse_decimal_component(components.next())?;
    let minor = parse_decimal_component(components.next())?;
    let patch = parse_patch_component(components.next())?;
    Ok(DeliveryGitVersion::new(major, minor, patch))
}

fn parse_decimal_component(value: Option<&str>) -> Result<u32, DeliveryGitProbeError> {
    let value = value.ok_or(DeliveryGitProbeError::CapabilityUnavailable)?;
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(DeliveryGitProbeError::CapabilityUnavailable);
    }
    value
        .parse()
        .map_err(|_| DeliveryGitProbeError::CapabilityUnavailable)
}

fn parse_patch_component(value: Option<&str>) -> Result<u32, DeliveryGitProbeError> {
    let value = value.ok_or(DeliveryGitProbeError::CapabilityUnavailable)?;
    let digits = value.bytes().take_while(u8::is_ascii_digit).count();
    if digits == 0 {
        return Err(DeliveryGitProbeError::CapabilityUnavailable);
    }
    value[..digits]
        .parse()
        .map_err(|_| DeliveryGitProbeError::CapabilityUnavailable)
}

fn parse_object_format(output: &[u8]) -> Result<DeliveryGitObjectFormat, DeliveryGitProbeError> {
    DeliveryGitObjectFormat::parse_exact_git_output(output)
        .ok_or(DeliveryGitProbeError::CapabilityUnavailable)
}

fn parse_object_id(
    output: &[u8],
    hexadecimal_length: usize,
) -> Result<ProbeGitObjectId, DeliveryGitProbeError> {
    ProbeGitObjectId::try_new(parse_one_line(output)?, hexadecimal_length)
        .ok_or(DeliveryGitProbeError::CapabilityUnavailable)
}

fn parse_one_line(output: &[u8]) -> Result<&str, DeliveryGitProbeError> {
    if output.contains(&0) {
        return Err(DeliveryGitProbeError::CapabilityUnavailable);
    }
    let value =
        std::str::from_utf8(output).map_err(|_| DeliveryGitProbeError::CapabilityUnavailable)?;
    let line = value
        .strip_suffix("\r\n")
        .or_else(|| value.strip_suffix('\n'))
        .unwrap_or(value);
    if line.is_empty()
        || line.contains(['\r', '\n'])
        || line.trim_matches(char::is_whitespace) != line
    {
        return Err(DeliveryGitProbeError::CapabilityUnavailable);
    }
    Ok(line)
}

fn require_clean_merge_tree(
    output: &[u8],
    expected_tree: &ProbeGitObjectId,
    hexadecimal_length: usize,
) -> Result<(), DeliveryGitProbeError> {
    if output.len() != hexadecimal_length + 2 || &output[hexadecimal_length..] != b"\0\0" {
        return Err(DeliveryGitProbeError::CapabilityUnavailable);
    }
    let tree = std::str::from_utf8(&output[..hexadecimal_length])
        .ok()
        .and_then(|value| ProbeGitObjectId::try_new(value, hexadecimal_length))
        .ok_or(DeliveryGitProbeError::CapabilityUnavailable)?;
    if tree == *expected_tree {
        Ok(())
    } else {
        Err(DeliveryGitProbeError::CapabilityUnavailable)
    }
}

fn probe_environment(
    repository: &ExecutionDirectory,
    empty_config: &DeliveryGitEmptyConfig,
) -> Result<ChildEnvironment, DeliveryGitProbeError> {
    #[cfg(windows)]
    let system_root = std::env::var_os("SYSTEMROOT")
        .or_else(|| std::env::var_os("WINDIR"))
        .map(PathBuf::from);
    #[cfg(unix)]
    let system_root = None;
    let platform = PlatformEnvironment::try_new(repository.path().to_owned(), system_root)
        .map_err(|_| DeliveryGitProbeError::InvalidConfiguration)?;
    let mut entries = ChildEnvironment::for_git(&platform).entries().clone();
    empty_config
        .apply_delivery_git_environment(&mut entries)
        .map_err(|_| DeliveryGitProbeError::CleanupUnproven)?;
    for key in ["HOME", "XDG_CONFIG_HOME"] {
        entries.insert(
            OsString::from(key),
            repository.path().as_os_str().to_owned(),
        );
    }
    #[cfg(windows)]
    entries.insert(
        OsString::from("USERPROFILE"),
        repository.path().as_os_str().to_owned(),
    );
    Ok(ChildEnvironment::from_entries(entries))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::ffi::OsStr;

    use super::*;

    #[test]
    fn version_parser_accepts_platform_suffixes_but_not_malformed_components() {
        let version = parse_git_version(b"git version 2.53.0.windows.1\n").unwrap();
        assert_eq!(
            (version.major(), version.minor(), version.patch()),
            (2, 53, 0)
        );
        let apple = parse_git_version(b"git version 2.45.3 (Apple Git-146)\n").unwrap();
        assert_eq!((apple.major(), apple.minor(), apple.patch()), (2, 45, 3));
        assert!(parse_git_version(b"git version 2.x.0\n").is_err());
        assert!(parse_git_version(b"git version 2.53\n").is_err());
    }

    #[test]
    fn clean_merge_tree_requires_the_double_nul_protocol_shape() {
        let oid = "1".repeat(40);
        let expected = ProbeGitObjectId::try_new(&oid, 40).unwrap();
        let mut valid = oid.as_bytes().to_vec();
        valid.extend_from_slice(b"\0\0");
        assert!(require_clean_merge_tree(&valid, &expected, 40).is_ok());

        valid.pop();
        assert!(require_clean_merge_tree(&valid, &expected, 40).is_err());
    }

    #[test]
    fn cleanup_failure_has_precedence_and_cannot_mint_a_probe_handle() {
        let executable =
            Arc::new(PinnedExecutable::open(std::env::current_exe().unwrap()).unwrap());
        let temporary = tempfile::tempdir().unwrap();
        let private_runtime =
            Arc::new(ExecutionDirectory::open(temporary.path().canonicalize().unwrap()).unwrap());
        let result = finish_probe(
            executable,
            private_runtime,
            Ok(ProbeFacts {
                version: DeliveryGitVersion::new(2, 53, 0),
                object_format: DeliveryGitObjectFormat::Sha1,
            }),
            Err(DeliveryGitProbeError::CleanupUnproven),
        );
        assert_eq!(result.unwrap_err(), DeliveryGitProbeError::CleanupUnproven);
    }

    #[test]
    fn command_identity_change_keeps_its_stable_probe_classification() {
        let error = map_process_error(ProcessError::CommandPolicy(
            CommandPolicyError::IdentityChanged,
        ));
        assert_eq!(error, DeliveryGitProbeError::ExecutableChanged);
    }

    #[test]
    fn probe_environment_rebinds_home_xdg_and_uses_only_typed_private_config() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().canonicalize().unwrap();
        let parent = Arc::new(ExecutionDirectory::open(&root).unwrap());
        let workspace = ProbeWorkspace::create(parent).unwrap();
        let repository = workspace.directory();
        let config = workspace.git_sandbox().unwrap();
        let environment = probe_environment(&repository, &config).unwrap();
        let entries = environment.entries();

        let mut expected_config = BTreeMap::new();
        config
            .apply_delivery_git_environment(&mut expected_config)
            .unwrap();
        for key in ["GIT_CONFIG_GLOBAL", "GIT_CONFIG_SYSTEM"] {
            assert_eq!(
                entries.get(OsStr::new(key)).map(OsString::as_os_str),
                expected_config
                    .get(OsStr::new(key))
                    .map(OsString::as_os_str),
            );
        }
        #[cfg(unix)]
        for key in ["GIT_CONFIG_GLOBAL", "GIT_CONFIG_SYSTEM"] {
            assert_eq!(
                entries.get(OsStr::new(key)).map(OsString::as_os_str),
                Some(OsStr::new("<coding-agent-delivery-empty-config>")),
                "the supervisor, rather than a raw namespace path, materializes {key}",
            );
        }
        assert_eq!(
            entries
                .get(OsStr::new("GIT_CONFIG_NOSYSTEM"))
                .map(OsString::as_os_str),
            Some(OsStr::new("1"))
        );
        assert_eq!(
            entries
                .get(OsStr::new("GIT_ATTR_NOSYSTEM"))
                .map(OsString::as_os_str),
            Some(OsStr::new("1"))
        );
        for key in ["HOME", "XDG_CONFIG_HOME"] {
            assert_eq!(
                entries.get(OsStr::new(key)).map(OsString::as_os_str),
                Some(repository.path().as_os_str()),
                "probe must not inherit host {key}",
            );
        }
        #[cfg(windows)]
        assert_eq!(
            entries
                .get(OsStr::new("USERPROFILE"))
                .map(OsString::as_os_str),
            Some(repository.path().as_os_str()),
        );
        for key in [
            "GIT_CONFIG_COUNT",
            "GIT_CONFIG_KEY_0",
            "GIT_CONFIG_VALUE_0",
            "GIT_CONFIG_PARAMETERS",
            "GIT_DIR",
            "GIT_WORK_TREE",
            "GIT_INDEX_FILE",
            "XDG_CONFIG_DIRS",
            "GIT_ASKPASS",
            "SSH_ASKPASS",
            "GIT_EDITOR",
            "EDITOR",
            "PAGER",
        ] {
            assert!(!entries.contains_key(OsStr::new(key)));
        }

        drop(repository);
        workspace.cleanup().unwrap();
    }
}
