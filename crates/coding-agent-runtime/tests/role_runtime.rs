mod support;

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use coding_agent_core::{
    AgentRuntime, ContextRedactor, DurableCheckpointAck, DurableEventAck, DurableRoleEvent,
    ModelRequest, PreparedModelProvider, PreparedProviderRequest, ProviderError,
    ReviewDiffCheckpoint, Role, RoleActionRuntime, RoleEngineFactory, RoleEvent, RoleEventSink,
    RoleRun, RoleRuntimeResult, RuntimeActionRequest, RuntimeError, ToolRequest,
    WorkspaceCheckpoint,
};
use coding_agent_runtime::{
    ProcessLimits, ProvisionedWorktree, RoleScopedEngineFactory, RoleScopedRuntime, RuntimeSession,
    RuntimeSessionLimits, ToolchainPaths, WorktreeIdentity, WorktreeLimits, WorktreeProvisioner,
    discover_toolchain,
};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

const PACKAGE: &str = "role_runtime_fixture";

struct IdentityRedactor;

impl ContextRedactor for IdentityRedactor {
    fn redact(&self, content: &str) -> String {
        content.to_owned()
    }
}

struct UnusedProvider;

impl PreparedModelProvider for UnusedProvider {
    fn prepare(
        &self,
        _request: ModelRequest,
    ) -> Result<Box<dyn PreparedProviderRequest>, ProviderError> {
        Err(ProviderError::new(
            "UNUSED_PROVIDER",
            "factory scope test never sends a model request",
            false,
        ))
    }
}

struct UnusedEvents;

#[async_trait::async_trait]
impl RoleEventSink for UnusedEvents {
    async fn emit(
        &self,
        _event: RoleEvent,
        _cancellation: CancellationToken,
    ) -> Result<(), RuntimeError> {
        Err(RuntimeError::new(
            "UNUSED_EVENTS",
            "factory scope test never emits an event",
            false,
        ))
    }

    async fn emit_durable(
        &self,
        _event: DurableRoleEvent,
        _cancellation: CancellationToken,
    ) -> Result<DurableEventAck, RuntimeError> {
        Err(RuntimeError::new(
            "UNUSED_EVENTS",
            "factory scope test never emits an event",
            false,
        ))
    }

    async fn flush_checkpoint(
        &self,
        _generation: u64,
        _cancellation: CancellationToken,
    ) -> Result<DurableCheckpointAck, RuntimeError> {
        Err(RuntimeError::new(
            "UNUSED_EVENTS",
            "factory scope test never flushes a checkpoint",
            false,
        ))
    }
}

fn redactor() -> Arc<dyn ContextRedactor> {
    Arc::new(IdentityRedactor)
}

#[tokio::test]
async fn role_runtime_enforces_independent_permissions_over_one_shared_session() {
    let fixture = Fixture::new().await;
    let provisioned = fixture.provision().await;
    let workspace = provisioned.cargo_workspace_path().to_owned();
    let session = Arc::new(fixture.session(&provisioned));

    let planner =
        RoleScopedRuntime::try_new(Role::Planner, 1, Arc::clone(&session), redactor()).unwrap();
    let executor =
        RoleScopedRuntime::try_new(Role::Executor, 1, Arc::clone(&session), redactor()).unwrap();
    let reviewer =
        RoleScopedRuntime::try_new(Role::Reviewer, 1, Arc::clone(&session), redactor()).unwrap();
    #[cfg(feature = "test-support")]
    {
        assert!(planner.shares_session_with(&session));
        assert!(executor.shares_session_with(&session));
        assert!(reviewer.shares_session_with(&session));
    }
    assert_ne!(planner.owner(), executor.owner());

    let listed = planner
        .invoke(
            RuntimeActionRequest::Tool(ToolRequest::ListFiles {
                path: String::new(),
                depth: 2,
                limit: 32,
            }),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert!(matches!(listed, RoleRuntimeResult::Tool(_)));

    let invalid_list = planner
        .invoke(
            RuntimeActionRequest::Tool(ToolRequest::ListFiles {
                path: String::new(),
                depth: 0,
                limit: 32,
            }),
            CancellationToken::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(invalid_list.code, "ROLE_RUNTIME_ACTION_NOT_ALLOWED");

    for denied in [
        RuntimeActionRequest::Tool(ToolRequest::ReplaceFile {
            path: "src/lib.rs".to_owned(),
            expected_sha256: None,
            content: "pub fn value() -> u32 { 2 }\n".to_owned(),
        }),
        RuntimeActionRequest::Tool(ToolRequest::GitStatus),
        RuntimeActionRequest::Tool(ToolRequest::CargoCheck {
            package: None,
            timeout_ms: 1,
        }),
    ] {
        let error = planner
            .invoke(denied, CancellationToken::new())
            .await
            .unwrap_err();
        assert_eq!(error.code, "ROLE_RUNTIME_ACTION_NOT_ALLOWED");
    }
    assert_eq!(
        std::fs::read_to_string(workspace.join("src/lib.rs")).unwrap(),
        "pub fn value() -> u32 { 1 }\n"
    );

    let replaced = executor
        .invoke(
            RuntimeActionRequest::Tool(ToolRequest::ReplaceFile {
                path: "generated.txt".to_owned(),
                expected_sha256: None,
                content: "executor-owned change\n".to_owned(),
            }),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    let RoleRuntimeResult::Tool(replaced) = replaced else {
        panic!("replace_file must return a ToolResult");
    };
    assert_eq!(
        replaced.status(),
        coding_agent_core::ToolStatus::Succeeded,
        "{}",
        replaced.content()
    );
    assert_eq!(
        std::fs::read_to_string(workspace.join("generated.txt")).unwrap(),
        "executor-owned change\n"
    );

    let fingerprint = session
        .workspace_fingerprint(CancellationToken::new())
        .await
        .unwrap();
    let checkpoint = WorkspaceCheckpoint::try_at_generation(1, fingerprint).unwrap();
    let review_checkpoint = ReviewDiffCheckpoint::from_workspace_checkpoint(&checkpoint);
    let provider: Arc<dyn PreparedModelProvider> = Arc::new(UnusedProvider);
    let events: Arc<dyn RoleEventSink> = Arc::new(UnusedEvents);
    let factory_redactor: Arc<dyn ContextRedactor> = Arc::new(IdentityRedactor);
    let factory = RoleScopedEngineFactory::new(
        Arc::clone(&provider),
        Arc::clone(&session),
        Arc::clone(&events),
        Arc::clone(&factory_redactor),
    );
    #[cfg(feature = "test-support")]
    {
        assert!(factory.shares_provider_with(&provider));
        assert!(factory.shares_runtime_session_with(&session));
        assert!(factory.shares_event_sink_with(&events));
        assert!(factory.shares_redactor_with(&factory_redactor));
    }
    factory
        .create_engine(RoleRun::try_new(Role::Planner, 1).unwrap(), None)
        .unwrap();
    factory
        .create_engine(RoleRun::try_new(Role::Executor, 1).unwrap(), None)
        .unwrap();
    factory
        .create_engine(
            RoleRun::try_new(Role::Reviewer, 1).unwrap(),
            Some(review_checkpoint.clone()),
        )
        .unwrap();
    let missing_review_checkpoint = factory
        .create_engine(RoleRun::try_new(Role::Reviewer, 1).unwrap(), None)
        .expect_err("Reviewer without checkpoint must fail closed");
    assert_eq!(
        missing_review_checkpoint.code,
        "ROLE_ENGINE_FACTORY_SCOPE_MISMATCH"
    );
    let unexpected_planner_checkpoint = factory
        .create_engine(
            RoleRun::try_new(Role::Planner, 1).unwrap(),
            Some(review_checkpoint.clone()),
        )
        .expect_err("Planner with review checkpoint must fail closed");
    assert_eq!(
        unexpected_planner_checkpoint.code,
        "ROLE_ENGINE_FACTORY_SCOPE_MISMATCH"
    );

    let reviewer = RoleScopedRuntime::try_with_review_checkpoint(
        Role::Reviewer,
        1,
        Arc::clone(&session),
        redactor(),
        review_checkpoint.clone(),
    )
    .unwrap();

    let executor_diff_error = executor
        .invoke(
            RuntimeActionRequest::ReviewDiffManifest {
                generation: review_checkpoint.generation(),
                workspace_digest: review_checkpoint.workspace_digest().clone(),
            },
            CancellationToken::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(executor_diff_error.code, "ROLE_RUNTIME_ACTION_NOT_ALLOWED");

    let reviewer_replace_error = reviewer
        .invoke(
            RuntimeActionRequest::Tool(ToolRequest::ReplaceFile {
                path: "reviewer.txt".to_owned(),
                expected_sha256: None,
                content: "reviewer must not write\n".to_owned(),
            }),
            CancellationToken::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(
        reviewer_replace_error.code,
        "ROLE_RUNTIME_ACTION_NOT_ALLOWED"
    );
    assert!(!workspace.join("reviewer.txt").exists());

    let manifest = reviewer
        .invoke(
            RuntimeActionRequest::ReviewDiffManifest {
                generation: review_checkpoint.generation(),
                workspace_digest: review_checkpoint.workspace_digest().clone(),
            },
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert!(matches!(
        manifest,
        RoleRuntimeResult::ReviewDiffManifest(manifest) if manifest.files().len() == 1
    ));

    let terminal_manifest = reviewer
        .terminal_review_diff_manifest(review_checkpoint.clone(), CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(terminal_manifest.files().len(), 1);
    let terminal_error = executor
        .terminal_review_diff_manifest(review_checkpoint.clone(), CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(terminal_error.code, "ROLE_RUNTIME_CHECKPOINT_MISMATCH");
    let wrong_checkpoint = WorkspaceCheckpoint::try_at_generation(2, fingerprint).unwrap();
    let terminal_error = reviewer
        .terminal_review_diff_manifest(
            ReviewDiffCheckpoint::from_workspace_checkpoint(&wrong_checkpoint),
            CancellationToken::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(terminal_error.code, "ROLE_RUNTIME_CHECKPOINT_MISMATCH");

    let unscoped_reviewer =
        RoleScopedRuntime::try_new(Role::Reviewer, 2, Arc::clone(&session), redactor()).unwrap();
    let error = unscoped_reviewer
        .invoke(
            RuntimeActionRequest::ReviewDiffManifest {
                generation: review_checkpoint.generation(),
                workspace_digest: review_checkpoint.workspace_digest().clone(),
            },
            CancellationToken::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.code, "ROLE_RUNTIME_CHECKPOINT_MISMATCH");

    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let error = planner
        .invoke(
            RuntimeActionRequest::Tool(ToolRequest::ReadFile {
                path: "src/lib.rs".to_owned(),
                start_line: 1,
                end_line: 1,
            }),
            cancelled,
        )
        .await
        .unwrap_err();
    assert_eq!(error.code, "COMMAND_CANCELLED");
}

struct Fixture {
    _temporary: TempDir,
    runtime_directory: PathBuf,
    repository: PathBuf,
    artifact_root: PathBuf,
    toolchain: ToolchainPaths,
}

impl Fixture {
    async fn new() -> Self {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().canonicalize().unwrap();
        let runtime_directory = root.join("runtime");
        let repository = root.join("repository");
        let artifact_root = root.join("artifacts");
        for directory in [&runtime_directory, &repository.join("src"), &artifact_root] {
            std::fs::create_dir_all(directory).unwrap();
        }

        git_ok(&repository, &["init", "--quiet"]);
        git_ok(&repository, &["config", "user.name", "Role Runtime Test"]);
        git_ok(
            &repository,
            &["config", "user.email", "role-runtime@example.invalid"],
        );
        std::fs::write(repository.join(".gitignore"), b"/target/\n").unwrap();
        std::fs::write(
            repository.join("Cargo.toml"),
            format!(
                "[workspace]\n\n[package]\nname = \"{PACKAGE}\"\nversion = \"0.1.0\"\nedition = \"2024\"\n"
            ),
        )
        .unwrap();
        std::fs::write(
            repository.join("Cargo.lock"),
            format!(
                "# This file is automatically @generated by Cargo.\n# It is not intended for manual editing.\nversion = 4\n\n[[package]]\nname = \"{PACKAGE}\"\nversion = \"0.1.0\"\n"
            ),
        )
        .unwrap();
        std::fs::write(
            repository.join("src/lib.rs"),
            b"pub fn value() -> u32 { 1 }\n",
        )
        .unwrap();
        git_ok(&repository, &["add", "--all"]);
        git_ok(
            &repository,
            &["commit", "--quiet", "--no-gpg-sign", "-m", "base"],
        );

        let toolchain = discover_toolchain(
            &runtime_directory,
            Some(&concrete_rustc()),
            Some(&path_executable(if cfg!(windows) {
                "git.exe"
            } else {
                "git"
            })),
        )
        .await
        .unwrap();
        Self {
            _temporary: temporary,
            runtime_directory,
            repository,
            artifact_root,
            toolchain,
        }
    }

    async fn provision(&self) -> ProvisionedWorktree {
        let identity = WorktreeIdentity::try_new("repository-1", "role-runtime-task", 1).unwrap();
        std::fs::create_dir_all(
            self.artifact_root
                .join(identity.relative_path())
                .parent()
                .unwrap(),
        )
        .unwrap();
        let provisioner = WorktreeProvisioner::from_trusted_paths(
            &self.toolchain,
            &self.repository,
            &self.repository,
            &self.artifact_root,
            &self.runtime_directory,
            process_limits(),
            WorktreeLimits::try_new(Duration::from_secs(15)).unwrap(),
        )
        .unwrap();
        let reservation = provisioner
            .prepare(identity, CancellationToken::new())
            .await
            .unwrap();
        provisioner
            .provision_reserved(reservation, CancellationToken::new())
            .await
            .unwrap()
    }

    fn session(&self, provisioned: &ProvisionedWorktree) -> RuntimeSession {
        RuntimeSession::from_provisioned_worktree(
            provisioned,
            &self.toolchain,
            &self.runtime_directory,
            RuntimeSessionLimits::project_2_defaults(),
        )
        .unwrap()
    }
}

fn process_limits() -> ProcessLimits {
    ProcessLimits::try_new(
        512 * 1024,
        512 * 1024,
        Duration::from_secs(30),
        Duration::from_secs(5),
    )
    .unwrap()
}

fn concrete_rustc() -> PathBuf {
    let output =
        support::command_output(Command::new("rustc").args(["--print", "sysroot"])).unwrap();
    assert!(output.status.success());
    PathBuf::from(String::from_utf8(output.stdout).unwrap().trim())
        .join("bin")
        .join(if cfg!(windows) { "rustc.exe" } else { "rustc" })
        .canonicalize()
        .unwrap()
}

fn path_executable(name: &str) -> PathBuf {
    std::env::split_paths(&std::env::var_os("PATH").unwrap())
        .map(|directory| directory.join(name))
        .find(|candidate| candidate.is_file())
        .unwrap()
        .canonicalize()
        .unwrap()
}

fn git_ok(repository: &Path, arguments: &[&str]) {
    let mut command = Command::new("git");
    command.current_dir(repository).args(arguments);
    let status = support::command_status(&mut command).unwrap();
    assert!(status.success(), "git {arguments:?} failed");
}
