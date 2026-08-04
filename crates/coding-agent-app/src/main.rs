#[cfg(all(not(debug_assertions), not(feature = "embedded-web")))]
compile_error!("release builds require the `embedded-web` feature");

use std::process::ExitCode;

#[cfg(feature = "test-support")]
use coding_agent_app::ProcessTestEnvironment;
use coding_agent_app::{
    NativeDialogService, StartupDependencies, StartupOutcome, launch,
    run_degraded_shutdown_warning_if_requested,
};

#[cfg(all(debug_assertions, not(feature = "embedded-web")))]
const VITE_PUBLIC_ORIGIN: &str = "http://127.0.0.1:5173";

fn main() -> ExitCode {
    if run_degraded_shutdown_warning_if_requested() {
        return ExitCode::SUCCESS;
    }

    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init();

    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(_) => {
            show_early_error("The asynchronous application runtime could not be created.");
            return ExitCode::FAILURE;
        }
    };

    let code = run_on_platform_main_thread(&runtime);
    runtime.shutdown_background();
    if code == 0 {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

#[cfg(not(target_os = "macos"))]
fn run_on_platform_main_thread(runtime: &tokio::runtime::Runtime) -> i32 {
    let dependencies = match startup_dependencies(Some(NativeDialogService::new())) {
        Ok(dependencies) => dependencies,
        Err(error) => {
            show_early_error(&error);
            return 1;
        }
    };
    runtime.block_on(run_application(dependencies))
}

#[cfg(target_os = "macos")]
fn run_on_platform_main_thread(runtime: &tokio::runtime::Runtime) -> i32 {
    let (dialog, host) = match NativeDialogService::new_on_main_thread() {
        Ok(parts) => parts,
        Err(_) => {
            show_early_error("The native dialog host could not be created on the main thread.");
            return 1;
        }
    };
    let dialog_host_keepalive = dialog.clone();
    let dependencies = match startup_dependencies(Some(dialog)) {
        Ok(dependencies) => dependencies,
        Err(error) => {
            show_early_error(&error);
            return 1;
        }
    };
    runtime.block_on(select_application_and_dialog_host(
        dialog_host_keepalive,
        run_application(dependencies),
        host.run(),
        || show_early_error("The native dialog host stopped unexpectedly."),
    ))
}

fn startup_dependencies(
    dialog: Option<NativeDialogService>,
) -> Result<StartupDependencies, String> {
    let dependencies = StartupDependencies::production(dialog);
    #[cfg(all(debug_assertions, not(feature = "embedded-web")))]
    let dependencies = dependencies.with_development_public_origin(VITE_PUBLIC_ORIGIN);
    #[cfg(feature = "test-support")]
    let dependencies = ProcessTestEnvironment::from_environment()
        .and_then(|environment| environment.apply(dependencies))
        .map_err(|error| format!("The isolated process-test configuration is invalid: {error}"))?;
    Ok(dependencies)
}

#[cfg(any(target_os = "macos", test))]
async fn select_application_and_dialog_host<K, A, H, F>(
    keepalive: K,
    application: A,
    host: H,
    host_stopped: F,
) -> i32
where
    A: std::future::Future<Output = i32>,
    H: std::future::Future<Output = ()>,
    F: FnOnce(),
{
    tokio::pin!(application);
    tokio::pin!(host);
    let code = tokio::select! {
        biased;
        code = &mut application => code,
        () = &mut host => {
            host_stopped();
            1
        }
    };
    drop(keepalive);
    code
}

async fn run_application(dependencies: StartupDependencies) -> i32 {
    match launch(dependencies).await {
        Ok(StartupOutcome::Primary(primary)) => {
            let signal = tokio::select! {
                signal = wait_for_shutdown_signal() => signal,
                () = primary.wait_for_quit_request() => Ok(()),
            };
            let outcome = primary.shutdown().await;
            if signal.is_err() {
                tracing::warn!(
                    error_code = "SIGNAL_LISTENER_FAILED",
                    "shutdown signal listener failed"
                );
                1
            } else {
                outcome.exit_code()
            }
        }
        Ok(StartupOutcome::Secondary(_)) => 0,
        Err(_) => 1,
    }
}

#[cfg(unix)]
async fn wait_for_shutdown_signal() -> std::io::Result<()> {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => result,
        signal = terminate.recv() => signal.ok_or_else(|| std::io::Error::other("termination signal stream closed")),
    }
}

#[cfg(windows)]
async fn wait_for_shutdown_signal() -> std::io::Result<()> {
    let mut close = tokio::signal::windows::ctrl_close()?;
    let mut shutdown = tokio::signal::windows::ctrl_shutdown()?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => result,
        signal = close.recv() => signal.ok_or_else(|| std::io::Error::other("console close signal stream closed")),
        signal = shutdown.recv() => signal.ok_or_else(|| std::io::Error::other("system shutdown signal stream closed")),
    }
}

#[cfg(not(any(unix, windows)))]
async fn wait_for_shutdown_signal() -> std::io::Result<()> {
    tokio::signal::ctrl_c().await
}

#[cfg(not(feature = "test-support"))]
fn show_early_error(body: &str) {
    let _ = rfd::MessageDialog::new()
        .set_level(rfd::MessageLevel::Error)
        .set_title("Coding Agent could not start")
        .set_description(body)
        .show();
}

#[cfg(feature = "test-support")]
fn show_early_error(body: &str) {
    eprintln!("Coding Agent could not start: {body}");
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use coding_agent_app::{NativeMessageSink, PlatformPaths, StartupPaths};

    use super::{StartupDependencies, run_application, select_application_and_dialog_host};

    #[tokio::test]
    async fn application_completion_wins_when_dialog_host_closes_at_the_same_boundary() {
        let reported = Arc::new(AtomicBool::new(false));
        let report = reported.clone();

        let code = select_application_and_dialog_host((), async { 0 }, async {}, move || {
            report.store(true, Ordering::SeqCst)
        })
        .await;

        assert_eq!(code, 0);
        assert!(!reported.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn dialog_keepalive_prevents_normal_application_exit_from_becoming_a_host_error() {
        let (keepalive, mut host_receiver) = tokio::sync::mpsc::unbounded_channel::<()>();
        let reported = Arc::new(AtomicBool::new(false));
        let report = reported.clone();

        let code = select_application_and_dialog_host(
            keepalive,
            async { 0 },
            async move { while host_receiver.recv().await.is_some() {} },
            move || report.store(true, Ordering::SeqCst),
        )
        .await;

        assert_eq!(code, 0);
        assert!(!reported.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn path_preparation_failure_crosses_the_executable_boundary_as_exit_one() {
        let messages = Arc::new(CountingMessages::default());
        let mut dependencies = StartupDependencies::production(None);
        dependencies.paths = Arc::new(PrepareDeniedPaths);
        dependencies.messages = messages.clone();

        assert_eq!(run_application(dependencies).await, 1);
        assert_eq!(messages.calls.load(Ordering::SeqCst), 1);
    }

    struct PrepareDeniedPaths;

    impl StartupPaths for PrepareDeniedPaths {
        fn discover(&self) -> io::Result<PlatformPaths> {
            Ok(PlatformPaths::new("unused-data", "unused-runtime"))
        }

        fn prepare_lock_parent(&self, _paths: &PlatformPaths) -> io::Result<()> {
            Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "injected lock-parent preparation failure",
            ))
        }

        fn prepare(&self, _paths: &PlatformPaths) -> io::Result<()> {
            Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "injected path preparation failure",
            ))
        }
    }

    #[derive(Default)]
    struct CountingMessages {
        calls: AtomicUsize,
    }

    impl NativeMessageSink for CountingMessages {
        fn show_error(&self, _title: &'static str, _body: String) {
            self.calls.fetch_add(1, Ordering::SeqCst);
        }
    }
}
