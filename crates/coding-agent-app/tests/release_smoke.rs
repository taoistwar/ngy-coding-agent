use std::env;
use std::ffi::OsString;
use std::fmt::Write as _;
use std::fs;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use coding_agent_app::{PlatformPaths, PrivateFile, RuntimeDescriptor, StartupPhase};
use serde::{Deserialize, Serialize};
use tempfile::TempDir;

const RELEASE_BINARY_ENV: &str = "CODING_AGENT_RELEASE_BINARY";
const STARTUP_TIMEOUT: Duration = Duration::from_secs(30);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);
const POLL_INTERVAL: Duration = Duration::from_millis(25);
const MAX_HTTP_RESPONSE_BYTES: usize = 4 * 1024 * 1024;
const STARTUP_ERROR_CODE_PREFIX: &[u8] = b"CODING_AGENT_STARTUP_ERROR_CODE=";
const MAX_STARTUP_ERROR_CODE_BYTES: usize = 96;
const MAX_STARTUP_DIAGNOSTIC_LINE_BYTES: usize =
    STARTUP_ERROR_CODE_PREFIX.len() + MAX_STARTUP_ERROR_CODE_BYTES + 1;
const STARTUP_DIAGNOSTIC_READ_CHUNK_BYTES: usize = 4 * 1024;
const MAX_STARTUP_DIAGNOSTIC_BYTES_PER_POLL: usize = 64 * 1024;
const MAX_RUNTIME_DIAGNOSTIC_ENTRIES: usize = 256;
const DELIVERY_PROBE_WORKSPACE_PREFIX: &str = ".coding-agent-delivery-probe-";

#[test]
#[ignore = "requires CODING_AGENT_RELEASE_BINARY to name an embedded production artifact"]
fn release_binary_starts_without_node_or_dist() {
    let source = release_binary_from_environment();
    let mut application = ReleaseApplication::start(&source);
    let descriptor = application.wait_for_descriptor();

    assert_eq!(
        descriptor.pid().get(),
        application.process_id(),
        "the private descriptor must belong to the copied release process"
    );
    let client = SmokeHttpClient::new(descriptor.port().get());
    assert_eq!(
        client
            .request("GET", "/_local/ready", &[], &[])
            .expect("contact launcher readiness without a secret")
            .status,
        401,
        "launcher readiness must reject a missing launcher secret"
    );

    let ready = client
        .request(
            "GET",
            "/_local/ready",
            &[("X-Launcher-Secret", descriptor.launcher_secret())],
            &[],
        )
        .expect("contact protected launcher readiness");
    assert_eq!(ready.status, 200, "the release listener must be ready");
    let ready: ReadyBody =
        serde_json::from_slice(&ready.body).expect("decode the bounded readiness response");
    assert_eq!(
        ready.instance_id,
        descriptor.instance_id().hyphenated().to_string(),
        "readiness and the descriptor must identify the same instance"
    );
    assert_eq!(
        ready.state,
        StartupPhase::Ready,
        "a published production descriptor must report ready"
    );

    let reopen = client
        .request(
            "POST",
            "/_local/reopen",
            &[("X-Launcher-Secret", descriptor.launcher_secret())],
            &[],
        )
        .expect("request a protected one-time browser URL");
    assert_eq!(reopen.status, 200, "launcher reopen must issue a grant");
    let reopen: ReopenBody =
        serde_json::from_slice(&reopen.body).expect("decode the bounded reopen response");
    assert!(
        !reopen.expires_at.is_empty(),
        "the reopen grant must carry an expiry"
    );
    let launch_grant = launch_token(&reopen.url, descriptor.port().get());

    let exchange_body = serde_json::to_vec(&ExchangeBody {
        token: &launch_grant.token,
    })
    .expect("encode the launch-token exchange body");
    let exchange_origin = if launch_grant.uses_listener_origin {
        client.origin()
    } else {
        "http://127.0.0.1:5173"
    };
    let exchange_headers = [
        ("Origin", exchange_origin),
        ("Content-Type", "application/json"),
    ];
    let exchange = client
        .request(
            "POST",
            "/api/session/exchange",
            &exchange_headers,
            &exchange_body,
        )
        .expect("exchange the one-time launch token");
    assert_eq!(exchange.status, 204, "the launch-token exchange must work");
    let session_cookie = session_cookie(&exchange);

    assert_eq!(
        client
            .request(
                "POST",
                "/api/session/exchange",
                &exchange_headers,
                &exchange_body,
            )
            .expect("replay the consumed one-time launch token")
            .status,
        401,
        "a browser launch token must be consumable exactly once"
    );

    let bootstrap = client
        .request(
            "GET",
            "/api/bootstrap",
            &[("Cookie", session_cookie.as_str())],
            &[],
        )
        .expect("fetch the authenticated bootstrap document");
    assert_eq!(bootstrap.status, 200, "bootstrap must be available");
    let bootstrap: BootstrapBody =
        serde_json::from_slice(&bootstrap.body).expect("decode the bounded bootstrap response");
    assert_eq!(
        bootstrap.service_state, "ready",
        "a fresh release must bootstrap in the ready state"
    );
    assert_eq!(
        bootstrap.max_concurrent_tasks, 2,
        "the production bootstrap must advertise the default concurrent-task limit"
    );
    assert_secret_shape(&bootstrap.csrf_token, "bootstrap CSRF token");
    assert!(bootstrap.repositories.is_empty());
    assert!(bootstrap.tasks.is_empty());
    assert_eq!(bootstrap.latest_event_id, 0);
    assert!(!bootstrap.server_started_at.is_empty());
    assert_eq!(
        bootstrap.service_state_generation, 1,
        "startup advances the service generation when it becomes ready"
    );
    assert_fresh_scheduler(
        &bootstrap.scheduler,
        &descriptor,
        &bootstrap.server_started_at,
        bootstrap.service_state_generation,
        bootstrap.max_concurrent_tasks,
    );

    let root = client
        .request(
            "GET",
            "/",
            &[("Cookie", session_cookie.as_str()), ("Accept", "text/html")],
            &[],
        )
        .expect("fetch the embedded production root");
    assert_eq!(
        root.status, 200,
        "the copied release must contain its Web UI"
    );
    assert!(
        launch_grant.uses_listener_origin,
        "an embedded production artifact must issue its browser URL on the random listener origin"
    );
    assert!(
        root.header("content-type")
            .is_some_and(|value| value.split(';').next() == Some("text/html")),
        "the embedded root must be HTML"
    );
    assert_eq!(
        root.header("cache-control"),
        Some("no-store"),
        "the embedded document shell must never be cached"
    );
    let root = std::str::from_utf8(&root.body).expect("embedded root is UTF-8 HTML");
    assert!(
        root.to_ascii_lowercase().contains("<!doctype html>")
            && root.contains("<title>NGY Coding Agent</title>")
            && root.contains("id=\"root\"")
            && root.contains("/assets/index-")
            && !root.contains("/src/main.tsx"),
        "the embedded artifact must contain the React document shell"
    );

    let rejected_quit_headers = [
        ("Cookie", session_cookie.as_str()),
        ("Origin", client.origin()),
    ];
    assert_eq!(
        client
            .request("POST", "/api/app/quit", &rejected_quit_headers, &[])
            .expect("attempt quit without CSRF")
            .status,
        403,
        "quit must remain CSRF protected"
    );
    application.assert_running();

    let quit_headers = [
        ("Cookie", session_cookie.as_str()),
        ("Origin", client.origin()),
        ("X-CSRF-Token", bootstrap.csrf_token.as_str()),
    ];
    let quit = client
        .request("POST", "/api/app/quit", &quit_headers, &[])
        .expect("send the protected quit request through response EOF");
    assert_eq!(quit.status, 202, "protected quit must be accepted");
    let quit: QuitBody =
        serde_json::from_slice(&quit.body).expect("decode the bounded quit response");
    assert_eq!(quit.status, "shutting_down");

    application.wait_for_clean_exit();
    application.wait_for_descriptor_removal();
    let temporary_root = application.finish_cleanup();
    assert!(
        !temporary_root.exists(),
        "release-smoke data must be removed after the child exits"
    );
}

fn assert_fresh_scheduler(
    scheduler: &SchedulerBody,
    descriptor: &RuntimeDescriptor,
    server_started_at: &str,
    service_state_generation: u64,
    max_concurrent_tasks: u32,
) {
    assert_eq!(scheduler.schema_version, 1);
    assert_eq!(
        scheduler.server_instance_id,
        descriptor.instance_id().hyphenated().to_string()
    );
    assert_eq!(scheduler.server_started_at, server_started_at);
    assert_eq!(scheduler.generation, 2);
    assert_eq!(scheduler.as_of_event_id, 0);
    assert_eq!(scheduler.service_state_generation, service_state_generation);
    assert_eq!(scheduler.admission_state, "running");
    assert_eq!(scheduler.limits.global, max_concurrent_tasks);
    assert_eq!(scheduler.limits.per_repository, max_concurrent_tasks);
    assert_eq!(scheduler.limits.queued, 32);
    assert!((1..=8).contains(&scheduler.limits.cargo_jobs_per_task));
    assert_eq!(scheduler.active_task_count, 0);
    assert_eq!(scheduler.queued_task_count, 0);
    assert!(scheduler.queued_tasks.is_empty());
    assert!(scheduler.stopping_tasks.is_empty());
    assert_eq!(scheduler.storage.state, "unavailable");
    assert_eq!(scheduler.storage.data.state, "unavailable");
    assert_eq!(scheduler.storage.runtime.state, "unavailable");
    assert!(scheduler.storage.repositories.is_empty());
}

fn release_binary_from_environment() -> PathBuf {
    let supplied = env::var_os(RELEASE_BINARY_ENV)
        .map(PathBuf::from)
        .expect("CODING_AGENT_RELEASE_BINARY must name the built release artifact");
    assert!(
        supplied.is_absolute(),
        "CODING_AGENT_RELEASE_BINARY must be absolute"
    );
    let source = supplied
        .canonicalize()
        .expect("canonicalize CODING_AGENT_RELEASE_BINARY");
    assert!(source.is_file(), "the release artifact must be a file");
    source
}

struct ReleaseApplication {
    temporary: Option<TempDir>,
    temporary_root: PathBuf,
    database_path: PathBuf,
    runtime_dir: PathBuf,
    descriptor_path: PathBuf,
    startup_diagnostics: StartupDiagnosticCapture,
    child: Option<Child>,
}

impl ReleaseApplication {
    fn start(source: &Path) -> Self {
        let temporary = tempfile::Builder::new()
            .prefix("coding-agent-release-smoke-")
            .tempdir()
            .expect("create the release-smoke temporary tree");
        let temporary_root = temporary.path().to_path_buf();
        let launch_dir = temporary_root.join("clean-launch");
        fs::create_dir(&launch_dir).expect("create the clean launch directory");
        let copied_binary = launch_dir.join(format!("coding-agent-app{}", env::consts::EXE_SUFFIX));
        fs::copy(source, &copied_binary).expect("copy only the release executable");

        let launch_entries = fs::read_dir(&launch_dir)
            .expect("read the clean launch directory")
            .collect::<Result<Vec<_>, _>>()
            .expect("enumerate the clean launch directory");
        assert_eq!(
            launch_entries.len(),
            1,
            "the process launch directory must contain only the copied executable"
        );
        assert!(copied_binary.is_file());
        assert!(
            !launch_dir.join("web").exists() && !launch_dir.join("dist").exists(),
            "web/dist must be unavailable beside the release executable"
        );

        let environment = IsolatedChildEnvironment::new(&temporary_root);
        assert_node_cannot_spawn(&environment, &launch_dir);

        let startup_diagnostics_path = temporary_root.join("startup-diagnostics.log");
        let startup_diagnostics_writer = PrivateFile::create_new(&startup_diagnostics_path)
            .expect("create the private startup-diagnostics capture");
        let startup_diagnostics = StartupDiagnosticCapture::new(startup_diagnostics_path);

        let mut command = Command::new(&copied_binary);
        command
            .current_dir(&launch_dir)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::from(startup_diagnostics_writer.into_file()));
        environment.apply(&mut command);
        let child = command
            .spawn()
            .expect("start the copied release executable without CLI arguments");

        Self {
            temporary: Some(temporary),
            temporary_root,
            database_path: environment.data_dir.join("coding-agent.sqlite3"),
            runtime_dir: environment.runtime_dir.clone(),
            descriptor_path: environment.runtime_dir.join("instance.json"),
            startup_diagnostics,
            child: Some(child),
        }
    }

    fn process_id(&self) -> u32 {
        self.child.as_ref().expect("release child is present").id()
    }

    fn wait_for_descriptor(&mut self) -> RuntimeDescriptor {
        let deadline = Instant::now() + STARTUP_TIMEOUT;
        loop {
            if let Ok(descriptor) = RuntimeDescriptor::read(&self.descriptor_path) {
                assert!(
                    self.database_path.is_file(),
                    "the release database must be inside the isolated application-data tree"
                );
                let isolated_root = self
                    .temporary_root
                    .canonicalize()
                    .expect("canonicalize the release-smoke root");
                for private_path in [&self.database_path, &self.descriptor_path] {
                    assert!(
                        private_path
                            .canonicalize()
                            .expect("canonicalize a private release-smoke path")
                            .starts_with(&isolated_root),
                        "every discovered application path must remain inside the isolated tree"
                    );
                }
                return descriptor;
            }
            if let Some(code) = self.startup_diagnostics.poll() {
                panic!(
                    "the release child reported a startup failure before publishing its private descriptor (code={code})"
                );
            }
            if let Some(status) = self
                .child
                .as_mut()
                .expect("release child is present")
                .try_wait()
                .expect("poll the release child during startup")
            {
                if let Some(code) = self.startup_diagnostics.poll() {
                    panic!(
                        "the release child reported a startup failure before publishing its private descriptor (code={code})"
                    );
                }
                panic!(
                    "the release child exited before publishing its private descriptor (success={})",
                    status.success()
                );
            }
            if Instant::now() >= deadline {
                let database_exists = safe_presence(self.database_path.try_exists());
                let delivery_probe_workspace_exists =
                    safe_presence(delivery_probe_workspace_exists(&self.runtime_dir));
                panic!(
                    "the release child did not publish its private descriptor before the deadline (database_exists={database_exists}, delivery_probe_workspace_exists={delivery_probe_workspace_exists})"
                );
            }
            thread::sleep(POLL_INTERVAL);
        }
    }

    fn assert_running(&mut self) {
        assert!(
            self.child
                .as_mut()
                .expect("release child is present")
                .try_wait()
                .expect("poll the release child")
                .is_none(),
            "a rejected quit request must leave the application running"
        );
    }

    fn wait_for_clean_exit(&mut self) {
        let deadline = Instant::now() + SHUTDOWN_TIMEOUT;
        let status = loop {
            let child = self.child.as_mut().expect("release child is present");
            if let Some(status) = child.try_wait().expect("poll release shutdown") {
                break status;
            }
            assert!(
                Instant::now() < deadline,
                "the release child did not exit after protected quit"
            );
            thread::sleep(POLL_INTERVAL);
        };
        assert_clean_exit(status);
        self.child.take();
    }

    fn wait_for_descriptor_removal(&self) {
        let deadline = Instant::now() + SHUTDOWN_TIMEOUT;
        while self.descriptor_path.exists() && Instant::now() < deadline {
            thread::sleep(POLL_INTERVAL);
        }
        assert!(
            !self.descriptor_path.exists(),
            "clean shutdown must remove the private runtime descriptor"
        );
    }

    fn finish_cleanup(mut self) -> PathBuf {
        assert!(self.child.is_none(), "cleanup requires a stopped child");
        let temporary = self
            .temporary
            .take()
            .expect("release-smoke temporary tree is present");
        let temporary_path = temporary.keep();
        try_remove_temporary_tree(&temporary_path).unwrap_or_else(|error| {
            panic!("release-smoke temporary data remained locked: {error}")
        });
        self.temporary_root.clone()
    }
}

struct StartupDiagnosticCapture {
    path: PathBuf,
    offset: u64,
    scanner: StartupDiagnosticScanner,
}

impl StartupDiagnosticCapture {
    fn new(path: PathBuf) -> Self {
        Self {
            path,
            offset: 0,
            scanner: StartupDiagnosticScanner::default(),
        }
    }

    fn poll(&mut self) -> Option<String> {
        if let Some(code) = self.scanner.code() {
            return Some(code.to_owned());
        }

        let mut file = fs::File::open(&self.path).ok()?;
        file.seek(SeekFrom::Start(self.offset)).ok()?;

        let mut buffer = [0u8; STARTUP_DIAGNOSTIC_READ_CHUNK_BYTES];
        let mut remaining = MAX_STARTUP_DIAGNOSTIC_BYTES_PER_POLL;
        while remaining > 0 {
            let requested = remaining.min(buffer.len());
            let read = match file.read(&mut buffer[..requested]) {
                Ok(0) => break,
                Ok(read) => read,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(_) => break,
            };
            self.offset = self.offset.saturating_add(read as u64);
            remaining -= read;
            self.scanner.ingest(&buffer[..read]);
            if let Some(code) = self.scanner.code() {
                return Some(code.to_owned());
            }
        }

        None
    }
}

#[derive(Default)]
struct StartupDiagnosticScanner {
    line: Vec<u8>,
    discarding_line: bool,
    code: Option<String>,
}

impl StartupDiagnosticScanner {
    fn ingest(&mut self, bytes: &[u8]) {
        for byte in bytes.iter().copied() {
            if self.code.is_some() {
                return;
            }
            if byte == b'\n' {
                if !self.discarding_line
                    && let Some(code) = parse_startup_error_code(&self.line)
                {
                    self.code = Some(code.to_owned());
                }
                self.line.clear();
                self.discarding_line = false;
            } else if !self.discarding_line {
                if self.line.len() < MAX_STARTUP_DIAGNOSTIC_LINE_BYTES {
                    self.line.push(byte);
                } else {
                    self.line.clear();
                    self.discarding_line = true;
                }
            }
        }
    }

    fn code(&self) -> Option<&str> {
        self.code.as_deref()
    }
}

fn parse_startup_error_code(line: &[u8]) -> Option<&str> {
    let line = line.strip_suffix(b"\r").unwrap_or(line);
    let code = line.strip_prefix(STARTUP_ERROR_CODE_PREFIX)?;
    if code.is_empty()
        || code.len() > MAX_STARTUP_ERROR_CODE_BYTES
        || !code
            .iter()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || *byte == b'_')
    {
        return None;
    }
    std::str::from_utf8(code).ok()
}

fn safe_presence(result: io::Result<bool>) -> &'static str {
    match result {
        Ok(true) => "yes",
        Ok(false) => "no",
        Err(_) => "unknown",
    }
}

fn delivery_probe_workspace_exists(runtime_dir: &Path) -> io::Result<bool> {
    let mut entries = fs::read_dir(runtime_dir)?;
    for _ in 0..MAX_RUNTIME_DIAGNOSTIC_ENTRIES {
        let Some(entry) = entries.next() else {
            return Ok(false);
        };
        let entry = entry?;
        let name = entry.file_name();
        let Some(suffix) = name
            .to_str()
            .and_then(|name| name.strip_prefix(DELIVERY_PROBE_WORKSPACE_PREFIX))
        else {
            continue;
        };
        let has_canonical_name = suffix.len() == 32
            && suffix
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'));
        if has_canonical_name && entry.file_type()?.is_dir() {
            return Ok(true);
        }
    }
    match entries.next().transpose()? {
        Some(_) => Err(io::Error::other("runtime diagnostic entry budget exceeded")),
        None => Ok(false),
    }
}

#[test]
fn startup_error_code_parser_accepts_only_the_bounded_canonical_shape() {
    assert_eq!(
        parse_startup_error_code(b"CODING_AGENT_STARTUP_ERROR_CODE=RUNNER_UNAVAILABLE"),
        Some("RUNNER_UNAVAILABLE")
    );
    assert_eq!(
        parse_startup_error_code(b"CODING_AGENT_STARTUP_ERROR_CODE=RUNTIME_CONFIG_INVALID\r"),
        Some("RUNTIME_CONFIG_INVALID")
    );

    for rejected in [
        b"CODING_AGENT_STARTUP_ERROR_CODE=".as_slice(),
        b"CODING_AGENT_STARTUP_ERROR_CODE=runner_unavailable".as_slice(),
        b"CODING_AGENT_STARTUP_ERROR_CODE=RUNNER-UNAVAILABLE".as_slice(),
        b"prefix CODING_AGENT_STARTUP_ERROR_CODE=RUNNER_UNAVAILABLE".as_slice(),
        b"CODING_AGENT_STARTUP_ERROR_CODE=RUNNER_UNAVAILABLE suffix".as_slice(),
    ] {
        assert_eq!(parse_startup_error_code(rejected), None);
    }

    let overlong = format!(
        "CODING_AGENT_STARTUP_ERROR_CODE={}",
        "A".repeat(MAX_STARTUP_ERROR_CODE_BYTES + 1)
    );
    assert_eq!(parse_startup_error_code(overlong.as_bytes()), None);
}

#[test]
fn startup_diagnostic_scanner_ignores_arbitrary_and_incomplete_output() {
    let mut scanner = StartupDiagnosticScanner::default();
    scanner.ingest(b"arbitrary stderr that must never be surfaced\n");
    scanner.ingest(b"CODING_AGENT_STARTUP_ERROR_");
    assert_eq!(scanner.code(), None, "an incomplete record is not accepted");
    scanner.ingest(b"CODE=DELIVERY_GIT_UNAVAILABLE\r\n");
    assert_eq!(scanner.code(), Some("DELIVERY_GIT_UNAVAILABLE"));

    let mut recovered = StartupDiagnosticScanner::default();
    recovered.ingest(&[b'X'; MAX_STARTUP_DIAGNOSTIC_LINE_BYTES + 1]);
    recovered.ingest(b"\nCODING_AGENT_STARTUP_ERROR_CODE=STORE_UNAVAILABLE\n");
    assert_eq!(recovered.code(), Some("STORE_UNAVAILABLE"));
}

#[test]
fn startup_diagnostic_capture_reads_a_framed_record_while_the_writer_is_open() {
    let temporary = tempfile::tempdir().expect("create startup diagnostic fixture");
    let path = temporary.path().join("startup-diagnostics.log");
    let mut writer = PrivateFile::create_new(&path)
        .expect("create private startup diagnostic fixture")
        .into_file();
    let mut capture = StartupDiagnosticCapture::new(path);

    writer
        .write_all(b"arbitrary stderr without a line ending")
        .expect("write arbitrary startup stderr");
    writer.flush().expect("flush arbitrary startup stderr");
    assert_eq!(capture.poll(), None);

    writer
        .write_all(b"\nCODING_AGENT_STARTUP_ERROR_CODE=STARTUP_STORE_UNAVAILABLE\n")
        .expect("write framed startup diagnostic");
    writer.flush().expect("flush framed startup diagnostic");
    assert_eq!(capture.poll().as_deref(), Some("STARTUP_STORE_UNAVAILABLE"));
}

impl Drop for ReleaseApplication {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        if let Some(temporary) = self.temporary.take() {
            let temporary_path = temporary.keep();
            let _ = try_remove_temporary_tree(&temporary_path);
        }
    }
}

fn assert_clean_exit(status: ExitStatus) {
    assert!(status.success(), "protected quit must produce a clean exit");
}

fn try_remove_temporary_tree(path: &Path) -> io::Result<()> {
    // A just-launched platform browser can briefly retain inherited Windows
    // directory handles even after the application process has exited. It is
    // not part of the smoke contract, so allow that launcher hand-off to drain
    // while still requiring bounded, observable cleanup of every test file.
    let deadline = Instant::now() + SHUTDOWN_TIMEOUT;
    loop {
        match fs::remove_dir_all(path) {
            Ok(()) => return Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(_) if Instant::now() < deadline => thread::sleep(POLL_INTERVAL),
            Err(error) => return Err(error),
        }
    }
}

struct IsolatedChildEnvironment {
    path: OsString,
    cargo_home: PathBuf,
    rustup_home: Option<PathBuf>,
    home: PathBuf,
    local_app_data: PathBuf,
    roaming_app_data: PathBuf,
    xdg_data: PathBuf,
    xdg_runtime: PathBuf,
    xdg_config: PathBuf,
    xdg_cache: PathBuf,
    xdg_state: PathBuf,
    temporary: PathBuf,
    data_dir: PathBuf,
    runtime_dir: PathBuf,
}

impl IsolatedChildEnvironment {
    fn new(root: &Path) -> Self {
        let original_path = env::var_os("PATH").expect("the test environment must define PATH");
        let retained_paths = env::split_paths(&original_path)
            .filter(|entry| !path_entry_provides_node(entry))
            .collect::<Vec<_>>();
        #[cfg(unix)]
        let retained_paths = {
            let mut retained_paths = retained_paths;
            retained_paths.insert(0, preserve_bootstrap_git_without_node(root, &original_path));
            retained_paths
        };
        assert!(
            !retained_paths.is_empty(),
            "filtering Node entries must preserve operating-system PATH entries"
        );
        let path = env::join_paths(&retained_paths).expect("join the Node-free child PATH");

        // The release application now composes the real offline coding runner
        // before publishing its listener. Isolating HOME must not accidentally
        // hide the already-installed Rust toolchain that production requires;
        // the smoke contract removes Node and external Web assets, not Cargo or
        // rustup. Resolve these trusted host directories before overriding the
        // child's platform home variables.
        let host_home = directories::BaseDirs::new()
            .expect("the release smoke requires a host home directory")
            .home_dir()
            .to_owned();
        let cargo_home = env::var_os("CARGO_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| host_home.join(".cargo"))
            .canonicalize()
            .expect("canonicalize the installed Cargo home");
        assert!(
            cargo_home.is_dir(),
            "the release smoke requires an installed Cargo home"
        );
        let rustup_home = match env::var_os("RUSTUP_HOME") {
            Some(path) => Some(
                PathBuf::from(path)
                    .canonicalize()
                    .expect("canonicalize the configured rustup home"),
            ),
            None => {
                let default = host_home.join(".rustup");
                default.is_dir().then(|| {
                    default
                        .canonicalize()
                        .expect("canonicalize the installed rustup home")
                })
            }
        };

        #[cfg(all(unix, not(target_os = "macos")))]
        assert!(
            Path::new("/bin/true").is_file(),
            "the Unix release smoke requires /bin/true as its controlled browser opener"
        );

        let home = root.join("os-home");
        // A fresh Windows process resolves Known Folders from USERPROFILE only
        // when the conventional children already exist. Keep these exact paths
        // in addition to setting LOCALAPPDATA/APPDATA below; otherwise the smoke
        // could accidentally discover the real user's initialized Known Folder.
        let local_app_data = home.join("AppData").join("Local");
        let roaming_app_data = home.join("AppData").join("Roaming");
        let xdg_data = root.join("xdg-data");
        let xdg_runtime = root.join("xdg-runtime");
        let xdg_config = root.join("xdg-config");
        let xdg_cache = root.join("xdg-cache");
        let xdg_state = root.join("xdg-state");
        let temporary = root.join("os-temp");

        for directory in [
            &home,
            &local_app_data,
            &roaming_app_data,
            &xdg_data,
            &xdg_runtime,
            &xdg_config,
            &xdg_cache,
            &xdg_state,
            &temporary,
        ] {
            fs::create_dir_all(directory).expect("create an isolated child environment directory");
        }
        #[cfg(windows)]
        block_persistent_browser_profiles(&local_app_data, &roaming_app_data);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;

            fs::set_permissions(&xdg_runtime, fs::Permissions::from_mode(0o700))
                .expect("make the isolated XDG runtime directory private");
        }

        #[cfg(windows)]
        let data_dir = local_app_data.join("ngy").join("coding-agent").join("data");
        #[cfg(target_os = "macos")]
        let data_dir = home
            .join("Library")
            .join("Application Support")
            .join("com.ngy.coding-agent");
        #[cfg(all(unix, not(target_os = "macos")))]
        let data_dir = xdg_data.join("coding-agent");
        #[cfg(not(any(unix, windows)))]
        let data_dir = xdg_data.join("coding-agent");

        #[cfg(all(unix, not(target_os = "macos")))]
        let runtime_dir = xdg_runtime.join("coding-agent");
        #[cfg(not(all(unix, not(target_os = "macos"))))]
        let runtime_dir = data_dir.join("run");

        let application_paths = PlatformPaths::new(data_dir.clone(), runtime_dir.clone());
        application_paths
            .prepare()
            .expect("prepare private release-smoke application paths");
        let mut provider = PrivateFile::create_new(data_dir.join("provider.json"))
            .expect("create private non-contacted provider configuration");
        provider
            .write_all(
                br#"{"base_url":"https://127.0.0.1:9/","model":"offline-smoke","api_key":"offline-smoke-secret","tool_choice_compatibility":"strict"}"#,
            )
            .expect("write release-smoke provider configuration");
        provider
            .as_file()
            .sync_all()
            .expect("flush release-smoke provider configuration");

        Self {
            path,
            cargo_home,
            rustup_home,
            home,
            local_app_data,
            roaming_app_data,
            xdg_data,
            xdg_runtime,
            xdg_config,
            xdg_cache,
            xdg_state,
            temporary,
            data_dir,
            runtime_dir,
        }
    }

    fn apply(&self, command: &mut Command) {
        command
            .env("PATH", &self.path)
            .env("CARGO_HOME", &self.cargo_home)
            .env("HOME", &self.home)
            .env("USERPROFILE", &self.home)
            .env("LOCALAPPDATA", &self.local_app_data)
            .env("APPDATA", &self.roaming_app_data)
            .env("XDG_DATA_HOME", &self.xdg_data)
            .env("XDG_RUNTIME_DIR", &self.xdg_runtime)
            .env("XDG_CONFIG_HOME", &self.xdg_config)
            .env("XDG_CACHE_HOME", &self.xdg_cache)
            .env("XDG_STATE_HOME", &self.xdg_state)
            .env("TMPDIR", &self.temporary)
            .env("TMP", &self.temporary)
            .env("TEMP", &self.temporary)
            .env_remove("NODE")
            .env_remove("NODE_PATH")
            .env_remove("CODING_AGENT_TEST_APP_DATA_DIR")
            .env_remove("CODING_AGENT_TEST_RUNTIME_DIR")
            .env_remove("CODING_AGENT_TEST_SCENARIO");
        if let Some(rustup_home) = &self.rustup_home {
            command.env("RUSTUP_HOME", rustup_home);
        } else {
            command.env_remove("RUSTUP_HOME");
        }

        // Linux browser discovery normally depends on commands such as
        // `xdg-open` that may share a PATH entry with Node. Use a controlled,
        // successful OS executable so removing every Node-providing entry
        // cannot also turn browser launch into a blocking error-dialog path.
        #[cfg(all(unix, not(target_os = "macos")))]
        command.env("BROWSER", "/bin/true");
    }
}

#[cfg(windows)]
fn block_persistent_browser_profiles(local_app_data: &Path, roaming_app_data: &Path) {
    // The production process is expected to attempt its normal browser launch.
    // A browser inheriting the isolated Windows profile could otherwise remain
    // alive after the app exits and keep that temporary profile locked. Files at
    // the conventional vendor roots make a fresh browser fail safely, while the
    // application's independent `ngy` subtree remains fully writable.
    for path in [
        local_app_data.join("Google"),
        local_app_data.join("Microsoft"),
        local_app_data.join("BraveSoftware"),
        local_app_data.join("Chromium"),
        local_app_data.join("Vivaldi"),
        local_app_data.join("Yandex"),
        roaming_app_data.join("Mozilla"),
        roaming_app_data.join("Opera Software"),
    ] {
        fs::write(path, b"browser profile disabled for release smoke")
            .expect("guard an isolated browser profile root");
    }
}

fn path_entry_provides_node(entry: &Path) -> bool {
    ["node", "node.exe"]
        .iter()
        .any(|executable| entry.join(executable).is_file())
}

#[cfg(unix)]
fn preserve_bootstrap_git_without_node(root: &Path, original_path: &OsString) -> PathBuf {
    // Hosted Unix images may install the supported Git and Node executables in
    // the same PATH directory. Dropping that entire entry proves Node is absent
    // but can silently substitute a different system Git for the production
    // startup capability probe. Publish only the already-selected Git object
    // through a private PATH entry; tool discovery canonicalizes and pins it.
    let git = env::split_paths(original_path)
        .map(|entry| entry.join("git"))
        .find(|candidate| candidate.is_file())
        .expect("the release smoke requires Git on the host PATH")
        .canonicalize()
        .expect("canonicalize the host Git executable");
    let bootstrap_bin = root.join("bootstrap-bin");
    fs::create_dir(&bootstrap_bin).expect("create the Node-free bootstrap directory");
    let bootstrap_git = bootstrap_bin.join("git");
    std::os::unix::fs::symlink(&git, &bootstrap_git)
        .expect("publish only Git in the Node-free bootstrap directory");
    assert_eq!(
        bootstrap_git
            .canonicalize()
            .expect("canonicalize the isolated Git bootstrap link"),
        git,
        "the isolated bootstrap entry must retain the host-selected Git object"
    );
    assert!(
        !bootstrap_bin.join("node").exists(),
        "the isolated bootstrap directory must not expose Node"
    );
    bootstrap_bin
}

fn assert_node_cannot_spawn(environment: &IsolatedChildEnvironment, current_dir: &Path) {
    // Probe command lookup inside the exact child environment. In particular,
    // do not accept a Node executable merely because it starts and exits with a
    // nonzero status.
    #[cfg(windows)]
    let mut command = {
        let system_root =
            env::var_os("SystemRoot").unwrap_or_else(|| OsString::from(r"C:\Windows"));
        let mut command = Command::new(
            PathBuf::from(system_root)
                .join("System32")
                .join("where.exe"),
        );
        command.arg("node");
        command
    };
    #[cfg(unix)]
    let mut command = {
        let mut command = Command::new("/bin/sh");
        command.args(["-c", "command -v node >/dev/null 2>&1"]);
        command
    };
    #[cfg(not(any(unix, windows)))]
    let mut command = {
        let mut command = Command::new("node");
        command.arg("--version");
        command
    };
    command
        .current_dir(current_dir)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    environment.apply(&mut command);
    let status = command
        .status()
        .expect("run the operating-system Node availability probe");
    assert!(
        !status.success(),
        "the Node command must not resolve in the release child environment"
    );
}

struct SmokeHttpClient {
    port: u16,
    host: String,
    origin: String,
}

impl SmokeHttpClient {
    fn new(port: u16) -> Self {
        Self {
            port,
            host: format!("127.0.0.1:{port}"),
            origin: format!("http://127.0.0.1:{port}"),
        }
    }

    fn origin(&self) -> &str {
        &self.origin
    }

    fn request(
        &self,
        method: &str,
        path: &str,
        headers: &[(&str, &str)],
        body: &[u8],
    ) -> io::Result<RawHttpResponse> {
        validate_request_component(method)?;
        validate_request_component(path)?;
        let address = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, self.port));
        let mut stream = TcpStream::connect_timeout(&address, REQUEST_TIMEOUT)?;
        stream.set_read_timeout(Some(REQUEST_TIMEOUT))?;
        stream.set_write_timeout(Some(REQUEST_TIMEOUT))?;

        let mut request = String::new();
        write!(&mut request, "{method} {path} HTTP/1.1\r\n")
            .map_err(|_| invalid_http("could not encode the HTTP request"))?;
        write!(&mut request, "Host: {}\r\n", self.host)
            .map_err(|_| invalid_http("could not encode the HTTP request"))?;
        request.push_str("Connection: close\r\n");
        for (name, value) in headers {
            validate_request_component(name)?;
            validate_request_component(value)?;
            write!(&mut request, "{name}: {value}\r\n")
                .map_err(|_| invalid_http("could not encode the HTTP request"))?;
        }
        write!(&mut request, "Content-Length: {}\r\n\r\n", body.len())
            .map_err(|_| invalid_http("could not encode the HTTP request"))?;
        stream.write_all(request.as_bytes())?;
        stream.write_all(body)?;
        stream.flush()?;

        let encoded = read_bounded_to_end(&mut stream)?;
        RawHttpResponse::parse(&encoded)
    }
}

fn validate_request_component(value: &str) -> io::Result<()> {
    if value.contains(['\r', '\n']) {
        Err(invalid_http("invalid HTTP request component"))
    } else {
        Ok(())
    }
}

fn read_bounded_to_end(stream: &mut TcpStream) -> io::Result<Vec<u8>> {
    let mut encoded = Vec::new();
    let mut buffer = [0_u8; 16 * 1024];
    loop {
        let count = stream.read(&mut buffer)?;
        if count == 0 {
            return Ok(encoded);
        }
        if count > MAX_HTTP_RESPONSE_BYTES.saturating_sub(encoded.len()) {
            return Err(invalid_http("HTTP response exceeds the smoke-test bound"));
        }
        encoded.extend_from_slice(&buffer[..count]);
    }
}

struct RawHttpResponse {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl RawHttpResponse {
    fn parse(encoded: &[u8]) -> io::Result<Self> {
        let header_end = find_bytes(encoded, b"\r\n\r\n")
            .ok_or_else(|| invalid_http("HTTP response headers are incomplete"))?;
        let header_bytes = &encoded[..header_end];
        let header_text = std::str::from_utf8(header_bytes)
            .map_err(|_| invalid_http("HTTP response headers are not UTF-8"))?;
        let mut lines = header_text.split("\r\n");
        let status_line = lines
            .next()
            .ok_or_else(|| invalid_http("HTTP response has no status line"))?;
        let mut status_parts = status_line.split_whitespace();
        let version = status_parts
            .next()
            .ok_or_else(|| invalid_http("HTTP response status is malformed"))?;
        if !matches!(version, "HTTP/1.1" | "HTTP/1.0") {
            return Err(invalid_http("HTTP response version is unsupported"));
        }
        let status = status_parts
            .next()
            .ok_or_else(|| invalid_http("HTTP response status is malformed"))?
            .parse::<u16>()
            .map_err(|_| invalid_http("HTTP response status is malformed"))?;

        let mut headers = Vec::new();
        for line in lines {
            let (name, value) = line
                .split_once(':')
                .ok_or_else(|| invalid_http("HTTP response header is malformed"))?;
            if name.is_empty() {
                return Err(invalid_http("HTTP response header name is empty"));
            }
            headers.push((name.to_ascii_lowercase(), value.trim().to_owned()));
        }

        let raw_body = &encoded[header_end + 4..];
        let transfer_encoding = unique_header(&headers, "transfer-encoding")?;
        let content_length = unique_header(&headers, "content-length")?;
        let body = if transfer_encoding.is_some_and(|value| value.eq_ignore_ascii_case("chunked")) {
            if content_length.is_some() {
                return Err(invalid_http(
                    "HTTP response uses both chunked and content-length framing",
                ));
            }
            decode_chunked(raw_body)?
        } else if let Some(content_length) = content_length {
            let expected = content_length
                .parse::<usize>()
                .map_err(|_| invalid_http("HTTP content length is malformed"))?;
            if raw_body.len() != expected {
                return Err(invalid_http("HTTP response body length is inconsistent"));
            }
            raw_body.to_vec()
        } else {
            raw_body.to_vec()
        };

        Ok(Self {
            status,
            headers,
            body,
        })
    }

    fn header(&self, name: &str) -> Option<&str> {
        let mut values = self
            .headers
            .iter()
            .filter(|(candidate, _)| candidate.eq_ignore_ascii_case(name));
        let first = values.next().map(|(_, value)| value.as_str())?;
        if values.next().is_some() {
            None
        } else {
            Some(first)
        }
    }
}

fn unique_header<'a>(headers: &'a [(String, String)], name: &str) -> io::Result<Option<&'a str>> {
    let mut values = headers
        .iter()
        .filter(|(candidate, _)| candidate.eq_ignore_ascii_case(name));
    let first = values.next().map(|(_, value)| value.as_str());
    if values.next().is_some() {
        Err(invalid_http("HTTP response duplicated a framing header"))
    } else {
        Ok(first)
    }
}

fn decode_chunked(mut encoded: &[u8]) -> io::Result<Vec<u8>> {
    let mut decoded = Vec::new();
    loop {
        let line_end = find_bytes(encoded, b"\r\n")
            .ok_or_else(|| invalid_http("chunked response has no size terminator"))?;
        let size_text = std::str::from_utf8(&encoded[..line_end])
            .map_err(|_| invalid_http("chunk size is not UTF-8"))?;
        let size_text = size_text.split(';').next().unwrap_or("").trim();
        let size = usize::from_str_radix(size_text, 16)
            .map_err(|_| invalid_http("chunk size is malformed"))?;
        encoded = &encoded[line_end + 2..];
        if size == 0 {
            if encoded == b"\r\n" || encoded.ends_with(b"\r\n\r\n") {
                return Ok(decoded);
            }
            return Err(invalid_http("chunked response trailers are malformed"));
        }
        if size > MAX_HTTP_RESPONSE_BYTES.saturating_sub(decoded.len())
            || encoded.len() < size + 2
            || &encoded[size..size + 2] != b"\r\n"
        {
            return Err(invalid_http("chunked response body is malformed"));
        }
        decoded.extend_from_slice(&encoded[..size]);
        encoded = &encoded[size + 2..];
    }
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn invalid_http(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

struct LaunchGrant {
    token: String,
    uses_listener_origin: bool,
}

fn launch_token(url: &str, port: u16) -> LaunchGrant {
    let listener_prefix = format!("http://127.0.0.1:{port}/#token=");
    let (token, uses_listener_origin) = if let Some(token) = url.strip_prefix(&listener_prefix) {
        (token, true)
    } else if let Some(token) = url.strip_prefix("http://127.0.0.1:5173/#token=") {
        // The intentional RED artifact is a default-feature debug build. It
        // advertises the one fixed Vite development origin, allowing the smoke
        // to reach the later embedded-static assertion before it fails.
        (token, false)
    } else {
        panic!("reopen URL must use an approved exact loopback origin and token fragment");
    };
    assert!(!token.is_empty(), "the reopen token must not be empty");
    assert_secret_shape(token, "one-time launch token");
    LaunchGrant {
        token: token.to_owned(),
        uses_listener_origin,
    }
}

fn session_cookie(response: &RawHttpResponse) -> String {
    let set_cookie = response
        .header("set-cookie")
        .expect("a successful exchange must set one session cookie");
    let cookie = set_cookie.split(';').next().unwrap_or("").trim();
    let value = cookie
        .strip_prefix("coding_agent_session=")
        .filter(|value| !value.is_empty())
        .expect("the exchange must set the process session cookie");
    assert_secret_shape(value, "process session cookie");
    let attributes = set_cookie
        .split(';')
        .skip(1)
        .map(|attribute| attribute.trim().to_ascii_lowercase())
        .collect::<Vec<_>>();
    for required in ["httponly", "samesite=strict", "path=/"] {
        assert!(
            attributes.iter().any(|attribute| attribute == required),
            "the process session cookie is missing a required security attribute"
        );
    }
    cookie.to_owned()
}

fn assert_secret_shape(secret: &str, label: &str) {
    assert!(
        secret.len() == 43
            && secret
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_')),
        "{label} must be a canonical URL-safe 32-byte secret"
    );
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadyBody {
    instance_id: String,
    state: StartupPhase,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReopenBody {
    url: String,
    expires_at: String,
}

#[derive(Serialize)]
struct ExchangeBody<'a> {
    token: &'a str,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BootstrapBody {
    csrf_token: String,
    repositories: Vec<serde_json::Value>,
    tasks: Vec<serde_json::Value>,
    latest_event_id: i64,
    server_started_at: String,
    service_state: String,
    service_state_generation: u64,
    max_concurrent_tasks: u32,
    scheduler: SchedulerBody,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SchedulerBody {
    schema_version: u16,
    server_instance_id: String,
    server_started_at: String,
    generation: u64,
    as_of_event_id: u64,
    service_state_generation: u64,
    admission_state: String,
    limits: SchedulerLimitsBody,
    active_task_count: u32,
    queued_task_count: u32,
    queued_tasks: Vec<serde_json::Value>,
    stopping_tasks: Vec<serde_json::Value>,
    storage: SchedulerStorageBody,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SchedulerLimitsBody {
    global: u32,
    per_repository: u32,
    queued: u32,
    cargo_jobs_per_task: u32,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SchedulerStorageBody {
    state: String,
    data: SchedulerStorageScopeBody,
    runtime: SchedulerStorageScopeBody,
    repositories: Vec<serde_json::Value>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SchedulerStorageScopeBody {
    state: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct QuitBody {
    status: String,
}
