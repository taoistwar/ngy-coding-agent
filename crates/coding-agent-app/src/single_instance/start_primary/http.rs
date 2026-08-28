use std::num::{NonZeroU16, NonZeroU32};
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use tokio::net::TcpListener;
use tokio::sync::Notify;
use tokio::time::Instant;

use crate::repository_service::DEFAULT_APPLICATION_WRITE_BUDGET;
use crate::security::{LaunchToken, LauncherSecret};
#[cfg(feature = "test-support")]
use crate::test_support::ActorPausePoint;
use crate::{
    ApplicationBackend, SecurityManager, SecuritySeed, build_application_api_router_with_delivery,
    build_runtime_router,
};

use super::ActorsReady;
#[cfg(feature = "test-support")]
use crate::single_instance::StartupDependencies;
use crate::single_instance::{
    PrimaryRuntime, RuntimeDescriptor, ServerRuntime, StartupError, StartupPhase,
    StartupPhaseController,
};

pub(super) async fn finish(actors: ActorsReady) -> Result<PrimaryRuntime, StartupError> {
    let network = NetworkReady::bind(actors).await?;
    let serving = network.install_http_server()?;
    serving.verify_publish_and_open_browser().await
}

struct NetworkReady {
    actors: ActorsReady,
    listener: TcpListener,
    port: NonZeroU16,
    browser_port: u16,
    security: SecurityManager,
    initial_launch_token: LaunchToken,
    launcher_secret: LauncherSecret,
}

impl NetworkReady {
    async fn bind(actors: ActorsReady) -> Result<Self, StartupError> {
        let seed = SecuritySeed::generate()?;
        let initial_launch_token = seed.initial_launch_token().clone();
        let launcher_secret = seed.launcher_secret().clone();
        let listener =
            crate::single_instance::bind_loopback(&*actors.context.dependencies.listeners).await?;
        let local_address = listener.local_addr().map_err(StartupError::Listener)?;
        let port = NonZeroU16::new(local_address.port()).ok_or(StartupError::InvalidListener)?;
        let listener_host = format!("127.0.0.1:{port}");
        let security = match &actors.context.dependencies.public_origin {
            crate::single_instance::StartupPublicOrigin::Listener => SecurityManager::from_seed(
                seed,
                format!("http://{listener_host}"),
                actors.context.dependencies.security_clock.clone(),
            ),
            crate::single_instance::StartupPublicOrigin::Development(public_origin) => {
                SecurityManager::from_seed_for_development(
                    seed,
                    public_origin.clone(),
                    listener_host,
                    actors.context.dependencies.security_clock.clone(),
                )
            }
        }?;
        let browser_port =
            crate::single_instance::canonical_loopback_origin_port(security.public_origin())
                .expect("SecurityManager guarantees a canonical nonzero loopback public origin");

        Ok(Self {
            actors,
            listener,
            port,
            browser_port,
            security,
            initial_launch_token,
            launcher_secret,
        })
    }

    fn install_http_server(mut self) -> Result<ServingPrimary, StartupError> {
        let quit_requested = Arc::new(Notify::new());
        let quit_signal = {
            let quit_requested = quit_requested.clone();
            Arc::new(move || quit_requested.notify_one()) as Arc<dyn Fn() + Send + Sync + 'static>
        };
        #[cfg(feature = "test-support")]
        let test_repository_registrar = self
            .actors
            .repository_registrar
            .as_ref()
            .expect("the repository registrar remains owned until backend construction")
            .clone();
        let backend = build_backend(&mut self.actors, &self.security, quit_signal);
        let (router, startup_phase) = build_server_router(&self.actors, &self.security, backend);
        let lock_keepalive = self
            .actors
            .lock_keepalive
            .take()
            .expect("the instance lock keepalive remains owned until server installation");
        let server = ServerRuntime::spawn(self.listener, router, lock_keepalive);
        self.actors.cleanup.install_server(server);
        let descriptor = RuntimeDescriptor::new(
            self.actors.context.instance_id,
            NonZeroU32::new(std::process::id()).expect("the process ID is nonzero"),
            self.port,
            self.actors.started_at,
            self.launcher_secret,
        )?;

        Ok(ServingPrimary {
            actors: self.actors,
            descriptor,
            startup_phase,
            quit_requested,
            browser_port: self.browser_port,
            initial_launch_token: self.initial_launch_token,
            #[cfg(feature = "test-support")]
            test_repository_registrar,
        })
    }
}

fn build_backend(
    actors: &mut ActorsReady,
    security: &SecurityManager,
    quit_signal: Arc<dyn Fn() + Send + Sync + 'static>,
) -> Arc<ApplicationBackend> {
    let repository_registrar = actors
        .repository_registrar
        .take()
        .expect("the repository registrar remains owned until backend construction");
    let repository_discovery = actors
        .repository_discovery
        .take()
        .expect("repository discovery remains owned until backend construction");
    let backend = ApplicationBackend::new_with_repository_runtime(
        actors.store.clone(),
        actors.writer.clone(),
        actors.dispatcher.clone(),
        actors.task_manager.clone(),
        repository_registrar,
        repository_discovery,
        actors.context.dependencies.dialog.clone(),
        security.clone(),
        actors.service_state.clone(),
        actors.mutation_gate.clone(),
        actors.started_at,
        actors.runner_selection.concurrency().get(),
        actors.max_queued_tasks,
        DEFAULT_APPLICATION_WRITE_BUDGET,
        quit_signal,
    );
    #[cfg(feature = "test-support")]
    let backend = match &actors.context.dependencies.process_test_support {
        Some(support) => backend.with_process_test_pauses(support.actor_pauses.clone()),
        None => backend,
    };
    Arc::new(backend)
}

fn build_server_router(
    actors: &ActorsReady,
    security: &SecurityManager,
    backend: Arc<ApplicationBackend>,
) -> (Router, StartupPhaseController) {
    let api_router = build_application_api_router_with_delivery(
        backend.clone(),
        Arc::new(security.clone()),
        actors.delivery_manager.clone(),
    );
    let startup_phase = StartupPhaseController::new();
    let router = build_runtime_router(
        api_router,
        actors.context.instance_id,
        startup_phase.clone(),
        security.clone(),
        actors.context.dependencies.wall_clock.clone(),
    );
    (router, startup_phase)
}

struct ServingPrimary {
    actors: ActorsReady,
    descriptor: RuntimeDescriptor,
    startup_phase: StartupPhaseController,
    quit_requested: Arc<Notify>,
    browser_port: u16,
    initial_launch_token: LaunchToken,
    #[cfg(feature = "test-support")]
    test_repository_registrar: crate::repository_service::RepositoryRuntimeRegistrar,
}

impl ServingPrimary {
    async fn verify_publish_and_open_browser(self) -> Result<PrimaryRuntime, StartupError> {
        self.verify_starting_probe().await?;
        if !self.startup_phase.mark_ready() {
            return Err(StartupError::SelfProbe);
        }
        if let Err(error) = self
            .descriptor
            .publish(&self.actors.context.paths.instance_descriptor)
        {
            let _ = std::fs::remove_file(&self.actors.context.paths.instance_descriptor);
            return Err(error.into());
        }
        if self
            .actors
            .service_state
            .set(crate::ServiceState::Ready)
            .is_err()
        {
            let _ = std::fs::remove_file(&self.actors.context.paths.instance_descriptor);
            return Err(StartupError::Runner(crate::StartupRunnerFactoryError::new(
                "STARTUP_ADMISSION_OPEN_FAILED",
            )));
        }
        #[cfg(feature = "test-support")]
        pause_after_descriptor(&self.actors.context.dependencies).await;

        let browser_opened = crate::single_instance::open_browser_or_report(
            &*self.actors.context.dependencies.browser,
            &*self.actors.context.dependencies.messages,
            self.browser_port,
            self.initial_launch_token.as_str(),
        );
        #[cfg(feature = "test-support")]
        let test_handles = crate::single_instance::PrimaryRuntimeTestHandles {
            store: self.actors.store.clone(),
            writer: self.actors.writer.clone(),
            dispatcher: self.actors.dispatcher.clone(),
            task_manager: self.actors.task_manager.clone(),
            delivery_manager: self.actors.delivery_manager.clone(),
            mutation_gate: self.actors.mutation_gate.clone(),
            repository_registrar: self.test_repository_registrar,
            process_liveness_scope: self.actors.instance_process_scope.clone(),
        };
        let shutdown = self.actors.shutdown_guard.disarm();

        Ok(PrimaryRuntime {
            descriptor: self.descriptor,
            _process_liveness_scope: self.actors.instance_process_scope,
            startup_phase: self.startup_phase,
            shutdown,
            quit_requested: self.quit_requested,
            browser_opened,
            #[cfg(feature = "test-support")]
            test_handles,
            #[cfg(feature = "test-support")]
            _test_signal_watchers: self.actors.test_signal_watchers,
            #[cfg(feature = "test-support")]
            _process_test_support: self
                .actors
                .context
                .dependencies
                .process_test_support
                .clone(),
        })
    }

    async fn verify_starting_probe(&self) -> Result<(), StartupError> {
        let self_probe_deadline = Instant::now() + Duration::from_secs(2);
        let (status, probe) =
            crate::local_client::probe_ready(&self.descriptor, self_probe_deadline)
                .await
                .map_err(|_| StartupError::SelfProbe)?;
        let probe = probe.ok_or(StartupError::SelfProbe)?;
        if status != http::StatusCode::OK
            || probe.instance_id != self.actors.context.instance_id
            || probe.state != StartupPhase::Starting
        {
            return Err(StartupError::SelfProbe);
        }
        Ok(())
    }
}

#[cfg(feature = "test-support")]
async fn pause_after_descriptor(dependencies: &StartupDependencies) {
    if let Some(support) = &dependencies.process_test_support {
        support
            .actor_pauses
            .pause(ActorPausePoint::DescriptorBeforeBrowser)
            .await;
    }
}
