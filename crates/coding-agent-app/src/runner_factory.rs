use std::fmt;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use coding_agent_core::AgentLimits;
use coding_agent_domain::Repository;
use coding_agent_provider::{ChatCompletionsClient, ClientLimits};
use coding_agent_runtime::{
    ProcessLimits, ToolchainPaths, WorktreeLimits, WorktreeProvisioner, discover_toolchain,
};
use coding_agent_store::Store;

use crate::{
    CodingAgentRunner, CodingAgentRunnerConfig, CodingAttemptError, PlatformPaths,
    Project2RuntimeSessionFactory, RepositoryWorktreeProvisionerFactory, StoreWriterHandle,
    TaskRunner, WallClock, WorktreeArtifactObserver, WorktreeCodingAgentAttemptFactory,
    load_provider_config, reconcile_restart_artifacts,
};

const PRODUCTION_CONCURRENCY: NonZeroU32 = NonZeroU32::new(1).unwrap();
const ARTIFACT_RECONCILIATION_TIMEOUT: Duration = Duration::from_secs(5);

/// Capabilities made available only after private paths are prepared, the
/// primary-instance lock is held, and the durable writer is running.
#[derive(Clone)]
pub struct StartupRunnerContext {
    paths: PlatformPaths,
    store: Store,
    writer: StoreWriterHandle,
    wall_clock: Arc<dyn WallClock>,
}

impl StartupRunnerContext {
    pub(crate) fn new(
        paths: PlatformPaths,
        store: Store,
        writer: StoreWriterHandle,
        wall_clock: Arc<dyn WallClock>,
    ) -> Self {
        Self {
            paths,
            store,
            writer,
            wall_clock,
        }
    }

    pub const fn paths(&self) -> &PlatformPaths {
        &self.paths
    }

    pub const fn store(&self) -> &Store {
        &self.store
    }

    pub const fn writer(&self) -> &StoreWriterHandle {
        &self.writer
    }

    pub fn wall_clock(&self) -> Arc<dyn WallClock> {
        Arc::clone(&self.wall_clock)
    }
}

/// One inseparable runner/concurrency selection returned by startup.
#[derive(Clone)]
pub struct StartupRunnerSelection {
    runner: Arc<dyn TaskRunner>,
    concurrency: NonZeroU32,
}

impl StartupRunnerSelection {
    pub fn new(runner: Arc<dyn TaskRunner>, concurrency: NonZeroU32) -> Self {
        Self {
            runner,
            concurrency,
        }
    }

    pub fn runner(&self) -> Arc<dyn TaskRunner> {
        Arc::clone(&self.runner)
    }

    pub const fn concurrency(&self) -> NonZeroU32 {
        self.concurrency
    }
}

#[async_trait::async_trait]
pub trait StartupRunnerFactory: Send + Sync + 'static {
    async fn create(
        &self,
        context: StartupRunnerContext,
    ) -> Result<StartupRunnerSelection, StartupRunnerFactoryError>;
}

/// Secret-safe startup failure. Detailed source errors remain behind the
/// composition boundary and must never include provider credentials or paths.
#[derive(Clone, PartialEq, Eq)]
pub struct StartupRunnerFactoryError {
    code: String,
    message: &'static str,
}

impl StartupRunnerFactoryError {
    pub fn new(code: impl Into<String>) -> Self {
        let code = code.into();
        let code = if valid_code(&code) {
            code
        } else {
            "RUNNER_STARTUP_FAILED".to_owned()
        };
        Self {
            code,
            message: "the coding task runner could not be started",
        }
    }

    pub fn code(&self) -> &str {
        &self.code
    }
}

impl fmt::Debug for StartupRunnerFactoryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StartupRunnerFactoryError")
            .field("code", &self.code)
            .field("message", &self.message)
            .finish()
    }
}

impl fmt::Display for StartupRunnerFactoryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.message)
    }
}

impl std::error::Error for StartupRunnerFactoryError {}

fn valid_code(code: &str) -> bool {
    !code.is_empty()
        && code.len() <= 96
        && code
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
}

/// Explicit fixed selection for tests; production never constructs this type.
pub struct FixedStartupRunnerFactory {
    selection: StartupRunnerSelection,
}

/// Production composition root. It is intentionally inert until the locked
/// primary invokes [`StartupRunnerFactory::create`].
#[derive(Debug, Clone, Copy, Default)]
pub struct ProductionStartupRunnerFactory;

struct ProductionWorktreeProvisioners {
    toolchain: Arc<ToolchainPaths>,
    artifact_root: std::path::PathBuf,
    temporary_directory: std::path::PathBuf,
    process_limits: ProcessLimits,
    worktree_limits: WorktreeLimits,
}

impl ProductionWorktreeProvisioners {
    fn provisioner_for(
        &self,
        repository: &Repository,
    ) -> Result<Arc<WorktreeProvisioner>, CodingAttemptError> {
        WorktreeProvisioner::from_trusted_paths(
            &self.toolchain,
            repository.git_root.as_path(),
            repository.cargo_workspace_root.as_path(),
            &self.artifact_root,
            &self.temporary_directory,
            self.process_limits,
            self.worktree_limits,
        )
        .map(Arc::new)
        .map_err(|error| CodingAttemptError::new(error.code(), false))
    }
}

impl RepositoryWorktreeProvisionerFactory for ProductionWorktreeProvisioners {
    fn create(
        &self,
        repository: &Repository,
    ) -> Result<Arc<WorktreeProvisioner>, CodingAttemptError> {
        self.provisioner_for(repository)
    }
}

#[async_trait::async_trait]
impl StartupRunnerFactory for ProductionStartupRunnerFactory {
    async fn create(
        &self,
        context: StartupRunnerContext,
    ) -> Result<StartupRunnerSelection, StartupRunnerFactoryError> {
        let provider_config = load_provider_config(context.paths())
            .map_err(|error| StartupRunnerFactoryError::new(error.code()))?;
        let provider = Arc::new(
            ChatCompletionsClient::new(provider_config, ClientLimits::default())
                .map_err(|error| StartupRunnerFactoryError::new(error.code))?,
        );
        let toolchain = Arc::new(
            discover_toolchain(context.paths().runtime_dir.as_path(), None, None)
                .await
                .map_err(|error| StartupRunnerFactoryError::new(error.code()))?,
        );
        let process_limits = production_process_limits();
        let worktree_limits = WorktreeLimits::try_new(Duration::from_secs(60))
            .map_err(|error| StartupRunnerFactoryError::new(error.code()))?;
        let provisioners = Arc::new(ProductionWorktreeProvisioners {
            toolchain: Arc::clone(&toolchain),
            artifact_root: context.paths().data_dir.clone(),
            temporary_directory: context.paths().runtime_dir.clone(),
            process_limits,
            worktree_limits,
        });

        let repositories = context
            .store()
            .list_repositories()
            .await
            .map_err(|_| StartupRunnerFactoryError::new("ARTIFACT_RECONCILIATION_FAILED"))?;
        let observer =
            WorktreeArtifactObserver::new(repositories.iter().filter_map(|repository| {
                provisioners
                    .provisioner_for(repository)
                    .ok()
                    .map(|provisioner| (repository.id, provisioner))
            }));
        reconcile_restart_artifacts(
            context.store(),
            context.writer(),
            &observer,
            ARTIFACT_RECONCILIATION_TIMEOUT,
        )
        .await
        .map_err(|_| StartupRunnerFactoryError::new("ARTIFACT_RECONCILIATION_FAILED"))?;

        let runtimes = Arc::new(Project2RuntimeSessionFactory::project_2_defaults(
            (*toolchain).clone(),
            context.paths().runtime_dir.clone(),
        ));
        let attempts = Arc::new(WorktreeCodingAgentAttemptFactory::new(
            provisioners,
            runtimes,
        ));
        let limits = AgentLimits::try_new(16, 32, 8 * 1024 * 1024, 256 * 1024)
            .expect("constant production agent limits are valid");
        let runner = Arc::new(CodingAgentRunner::new(
            context.writer().clone(),
            provider,
            attempts,
            context.wall_clock(),
            limits,
            CodingAgentRunnerConfig::default(),
        ));
        Ok(StartupRunnerSelection::new(runner, PRODUCTION_CONCURRENCY))
    }
}

fn production_process_limits() -> ProcessLimits {
    ProcessLimits::try_new(
        512 * 1024,
        256 * 1024,
        Duration::from_secs(10 * 60),
        Duration::from_secs(5),
    )
    .expect("constant production process limits are valid")
}

impl FixedStartupRunnerFactory {
    pub fn new(runner: Arc<dyn TaskRunner>, concurrency: NonZeroU32) -> Self {
        Self {
            selection: StartupRunnerSelection::new(runner, concurrency),
        }
    }
}

#[async_trait::async_trait]
impl StartupRunnerFactory for FixedStartupRunnerFactory {
    async fn create(
        &self,
        _context: StartupRunnerContext,
    ) -> Result<StartupRunnerSelection, StartupRunnerFactoryError> {
        Ok(self.selection.clone())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;
    use std::num::NonZeroU32;
    use std::sync::Arc;

    use coding_agent_store::Store;

    use super::{
        PRODUCTION_CONCURRENCY, ProductionStartupRunnerFactory, StartupRunnerContext,
        StartupRunnerFactory, StartupRunnerFactoryError, StartupRunnerSelection,
    };
    use crate::{
        EventDispatcherHandle, FakeTaskRunner, PlatformPaths, PrivateFile, StoreWriterHandle,
        SystemWallClock,
    };

    #[test]
    fn startup_errors_reject_untrusted_codes_without_echoing_them() {
        let error = StartupRunnerFactoryError::new("bad code with provider-secret");

        assert_eq!(error.code(), "RUNNER_STARTUP_FAILED");
        assert!(!format!("{error:?}").contains("provider-secret"));
        assert!(!format!("{error}").contains("provider-secret"));
    }

    #[test]
    fn runner_and_configured_concurrency_are_one_selection() {
        let concurrency = NonZeroU32::new(3).unwrap();
        let selection =
            StartupRunnerSelection::new(Arc::new(FakeTaskRunner::default()), concurrency);

        assert_eq!(selection.concurrency(), concurrency);
        let _ = selection.runner();
        assert_eq!(PRODUCTION_CONCURRENCY.get(), 1);
    }

    #[tokio::test]
    async fn production_factory_builds_a_noncontacted_https_runner_with_concurrency_one() {
        let temporary = tempfile::tempdir().unwrap();
        let paths = PlatformPaths::new(
            temporary.path().join("data"),
            temporary.path().join("runtime"),
        );
        std::fs::create_dir_all(&paths.data_dir).unwrap();
        std::fs::create_dir_all(&paths.runtime_dir).unwrap();
        let mut provider = PrivateFile::create_new(paths.data_dir.join("provider.json")).unwrap();
        provider
            .write_all(
                br#"{"base_url":"https://127.0.0.1:9/","model":"offline-unit","api_key":"offline-unit-secret"}"#,
            )
            .unwrap();
        provider.as_file().sync_all().unwrap();

        let store = Store::open(&paths.database_path).await.unwrap();
        store.migrate().await.unwrap();
        let dispatcher = EventDispatcherHandle::spawn(store.clone(), 16)
            .await
            .unwrap();
        let writer = StoreWriterHandle::spawn(store.clone(), Arc::new(dispatcher), 16);
        let selection = ProductionStartupRunnerFactory
            .create(StartupRunnerContext::new(
                paths,
                store,
                writer,
                Arc::new(SystemWallClock),
            ))
            .await
            .unwrap();

        assert_eq!(selection.concurrency().get(), 1);
        let _ = selection.runner();
    }
}
