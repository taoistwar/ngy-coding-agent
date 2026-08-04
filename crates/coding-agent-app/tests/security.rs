#![cfg(feature = "test-support")]

mod support;

use std::io::{self, Write};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use coding_agent_api::{
    ApiError, AuthContext, BootstrapResponse, RequestSecurity, SchedulerAdmissionStateDto,
    SchedulerLimitsDto, SchedulerStateDto, SchedulerStorageDto, SchedulerStorageScopeDto,
    SchedulerStorageStateDto, ServiceStateDto, SessionExchange, SessionExchangeRequest,
};
use coding_agent_app::{SecurityClock, SecurityManager, SecuritySeed};
use http::header::{ACCESS_CONTROL_ALLOW_ORIGIN, COOKIE, HOST, ORIGIN, SET_COOKIE};
use http::{HeaderName, HeaderValue, Method, Request, StatusCode};

const CSRF_HEADER: &str = "x-csrf-token";
const LAUNCHER_HEADER: &str = "x-launcher-secret";

#[tokio::test]
async fn concurrent_launch_token_exchange_has_exactly_one_winner() {
    let fixture = support::SecurityFixture::production();
    let token = fixture.initial_launch_token.as_str().to_owned();
    let mut exchanges = Vec::new();

    for _ in 0..32 {
        let manager = fixture.manager.clone();
        let token = token.clone();
        let host = fixture.expected_host.clone();
        let origin = fixture.public_origin.clone();
        exchanges.push(tokio::spawn(async move {
            let parts = request_parts(Method::POST, Some(&host), Some(&origin));
            RequestSecurity::exchange(&manager, &parts, &token).await
        }));
    }

    let mut successes = 0;
    for exchange in exchanges {
        if exchange.await.expect("exchange task joins").is_ok() {
            successes += 1;
        }
    }

    assert_eq!(successes, 1, "a launch token is consumed atomically");
}

#[tokio::test]
async fn rejected_host_and_origin_do_not_consume_the_launch_token() {
    let fixture = support::SecurityFixture::production();
    let token = fixture.initial_launch_token.as_str();

    let wrong_host = request_parts(
        Method::POST,
        Some("localhost:43121"),
        Some(&fixture.public_origin),
    );
    assert_error_code(
        RequestSecurity::exchange(&fixture.manager, &wrong_host, token).await,
        "SECURITY_INVALID_HOST",
    );

    let wrong_origin = request_parts(
        Method::POST,
        Some(&fixture.expected_host),
        Some("http://127.0.0.1:43122"),
    );
    assert_error_code(
        RequestSecurity::exchange(&fixture.manager, &wrong_origin, token).await,
        "SECURITY_INVALID_ORIGIN",
    );

    let valid = request_parts(
        Method::POST,
        Some(&fixture.expected_host),
        Some(&fixture.public_origin),
    );
    RequestSecurity::exchange(&fixture.manager, &valid, token)
        .await
        .expect("preflight rejections leave the token usable");
}

#[tokio::test]
async fn launch_tokens_expire_at_the_two_minute_boundary() {
    let before_boundary = support::SecurityFixture::production();
    before_boundary
        .clock
        .advance(Duration::from_secs(120) - Duration::from_nanos(1));
    exchange_initial(&before_boundary)
        .await
        .expect("token is live strictly before the boundary");

    let at_boundary = support::SecurityFixture::production();
    at_boundary.clock.advance(Duration::from_secs(120));
    assert_error_code(
        exchange_initial(&at_boundary).await,
        "SECURITY_INVALID_LAUNCH_TOKEN",
    );

    let issued_at = at_boundary.clock.now();
    let issued = at_boundary
        .manager
        .issue_launch_token()
        .expect("issue reopen token");
    assert_eq!(
        at_boundary.manager.launch_token_expires_at(&issued),
        Some(issued_at + Duration::from_secs(120)),
        "callers can publish the exact one-time-token expiry"
    );
}

#[tokio::test]
async fn exchange_sets_a_strict_host_only_cookie_and_independent_sessions() {
    let fixture = support::SecurityFixture::production();
    let first = establish_initial_session(&fixture).await;
    assert!(first.set_cookie.starts_with("coding_agent_session="));
    assert!(first.set_cookie.contains("; HttpOnly"));
    assert!(first.set_cookie.contains("; SameSite=Strict"));
    assert!(first.set_cookie.contains("; Path=/"));
    for forbidden in ["domain=", "expires=", "secure"] {
        assert!(
            !first.set_cookie.to_ascii_lowercase().contains(forbidden),
            "cookie must omit {forbidden}"
        );
    }

    let second_token = fixture
        .manager
        .issue_launch_token()
        .expect("issue second launch token");
    let second_exchange = exchange_token(&fixture, second_token.as_str())
        .await
        .expect("exchange second launch token");
    let second = authorize_exchange(&fixture.manager, &fixture.expected_host, second_exchange);
    assert_ne!(first.cookie, second.cookie);
    assert_ne!(first.csrf, second.csrf);

    for secret in [
        fixture.initial_launch_token.as_str(),
        fixture.manager.launcher_secret().as_str(),
        cookie_value(&first.cookie),
        &first.csrf,
    ] {
        let decoded = URL_SAFE_NO_PAD.decode(secret).expect("URL-safe secret");
        assert_eq!(decoded.len(), 32, "each secret has 256 random bits");
        assert_eq!(secret.len(), 43, "URL_SAFE_NO_PAD has a fixed length");
    }
}

#[tokio::test]
async fn read_and_mutation_authorization_require_the_full_matrix() {
    let fixture = support::SecurityFixture::production();
    let session = establish_initial_session(&fixture).await;

    let read = session_request(
        Method::GET,
        Some(&fixture.expected_host),
        Some(&session.cookie),
        None,
        None,
    );
    let auth = RequestSecurity::authorize_read(&fixture.manager, &read)
        .expect("cookie authorizes a public read");
    assert_eq!(
        fixture
            .manager
            .csrf_for_auth(&auth)
            .expect("resolve authenticated CSRF"),
        session.csrf
    );

    let mutation = session_request(
        Method::POST,
        Some(&fixture.expected_host),
        Some(&session.cookie),
        Some(&fixture.public_origin),
        Some(&session.csrf),
    );
    RequestSecurity::authorize_mutation(&fixture.manager, &mutation)
        .expect("session, exact origin, and CSRF authorize mutation");

    let cases = [
        (
            session_request(Method::GET, None, Some(&session.cookie), None, None),
            "SECURITY_INVALID_HOST",
            false,
        ),
        (
            session_request(
                Method::GET,
                Some("localhost:43121"),
                Some(&session.cookie),
                None,
                None,
            ),
            "SECURITY_INVALID_HOST",
            false,
        ),
        (
            session_request(Method::GET, Some(&fixture.expected_host), None, None, None),
            "SECURITY_INVALID_SESSION",
            false,
        ),
        (
            session_request(
                Method::GET,
                Some(&fixture.expected_host),
                Some("coding_agent_session=forged"),
                None,
                None,
            ),
            "SECURITY_INVALID_SESSION",
            false,
        ),
        (
            session_request(
                Method::POST,
                Some(&fixture.expected_host),
                Some(&session.cookie),
                None,
                Some(&session.csrf),
            ),
            "SECURITY_INVALID_ORIGIN",
            true,
        ),
        (
            session_request(
                Method::POST,
                Some(&fixture.expected_host),
                Some(&session.cookie),
                Some("http://127.0.0.1:43122"),
                Some(&session.csrf),
            ),
            "SECURITY_INVALID_ORIGIN",
            true,
        ),
        (
            session_request(
                Method::POST,
                Some(&fixture.expected_host),
                Some(&session.cookie),
                Some(&fixture.public_origin),
                None,
            ),
            "SECURITY_INVALID_CSRF",
            true,
        ),
        (
            session_request(
                Method::POST,
                Some(&fixture.expected_host),
                Some(&session.cookie),
                Some(&fixture.public_origin),
                Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
            ),
            "SECURITY_INVALID_CSRF",
            true,
        ),
    ];

    for (parts, expected, mutation) in cases {
        let result = if mutation {
            RequestSecurity::authorize_mutation(&fixture.manager, &parts)
        } else {
            RequestSecurity::authorize_read(&fixture.manager, &parts)
        };
        assert_error_code(result, expected);
    }
}

#[tokio::test]
async fn a_fresh_manager_invalidates_every_previous_process_secret() {
    let old = support::SecurityFixture::production();
    let old_session = establish_initial_session(&old).await;
    let old_launcher = old.manager.launcher_secret().as_str().to_owned();
    let old_unused_token = old
        .manager
        .issue_launch_token()
        .expect("issue old process token");

    let fresh = support::SecurityFixture::production();
    let old_read = session_request(
        Method::GET,
        Some(&fresh.expected_host),
        Some(&old_session.cookie),
        None,
        None,
    );
    assert_error_code(
        RequestSecurity::authorize_read(&fresh.manager, &old_read),
        "SECURITY_INVALID_SESSION",
    );

    let exchange = request_parts(
        Method::POST,
        Some(&fresh.expected_host),
        Some(&fresh.public_origin),
    );
    assert_error_code(
        RequestSecurity::exchange(&fresh.manager, &exchange, old_unused_token.as_str()).await,
        "SECURITY_INVALID_LAUNCH_TOKEN",
    );

    let old_launcher_request = launcher_request(&fresh.expected_host, &old_launcher);
    assert_error_code(
        fresh.manager.authorize_launcher(&old_launcher_request),
        "SECURITY_INVALID_LAUNCHER_SECRET",
    );

    let fresh_session = establish_initial_session(&fresh).await;
    let old_csrf = session_request(
        Method::POST,
        Some(&fresh.expected_host),
        Some(&fresh_session.cookie),
        Some(&fresh.public_origin),
        Some(&old_session.csrf),
    );
    assert_error_code(
        RequestSecurity::authorize_mutation(&fresh.manager, &old_csrf),
        "SECURITY_INVALID_CSRF",
    );
}

#[tokio::test]
async fn every_duplicated_security_header_and_cookie_is_rejected() {
    let fixture = support::SecurityFixture::production();

    let mut duplicated_origin = request_parts(
        Method::POST,
        Some(&fixture.expected_host),
        Some(&fixture.public_origin),
    );
    append_header(
        &mut duplicated_origin,
        ORIGIN.as_str(),
        &fixture.public_origin,
    );
    assert_error_code(
        RequestSecurity::exchange(
            &fixture.manager,
            &duplicated_origin,
            fixture.initial_launch_token.as_str(),
        )
        .await,
        "SECURITY_DUPLICATE_HEADER",
    );
    exchange_initial(&fixture)
        .await
        .expect("duplicate Origin does not consume the token");

    let session_fixture = support::SecurityFixture::production();
    let session = establish_initial_session(&session_fixture).await;

    let mut duplicated_host = session_request(
        Method::GET,
        Some(&session_fixture.expected_host),
        Some(&session.cookie),
        None,
        None,
    );
    append_header(
        &mut duplicated_host,
        HOST.as_str(),
        &session_fixture.expected_host,
    );
    assert_error_code(
        RequestSecurity::authorize_read(&session_fixture.manager, &duplicated_host),
        "SECURITY_DUPLICATE_HEADER",
    );

    let mut duplicated_cookie_header = session_request(
        Method::GET,
        Some(&session_fixture.expected_host),
        Some(&session.cookie),
        None,
        None,
    );
    append_header(
        &mut duplicated_cookie_header,
        COOKIE.as_str(),
        &session.cookie,
    );
    assert_error_code(
        RequestSecurity::authorize_read(&session_fixture.manager, &duplicated_cookie_header),
        "SECURITY_DUPLICATE_HEADER",
    );

    let duplicate_cookie_value = format!("{}; {}", session.cookie, session.cookie);
    let duplicated_named_cookie = session_request(
        Method::GET,
        Some(&session_fixture.expected_host),
        Some(&duplicate_cookie_value),
        None,
        None,
    );
    assert_error_code(
        RequestSecurity::authorize_read(&session_fixture.manager, &duplicated_named_cookie),
        "SECURITY_DUPLICATE_HEADER",
    );

    let mut duplicated_csrf = session_request(
        Method::POST,
        Some(&session_fixture.expected_host),
        Some(&session.cookie),
        Some(&session_fixture.public_origin),
        Some(&session.csrf),
    );
    append_header(&mut duplicated_csrf, CSRF_HEADER, &session.csrf);
    assert_error_code(
        RequestSecurity::authorize_mutation(&session_fixture.manager, &duplicated_csrf),
        "SECURITY_DUPLICATE_HEADER",
    );

    let launcher = session_fixture.manager.launcher_secret().as_str();
    let mut duplicated_launcher = launcher_request(&session_fixture.expected_host, launcher);
    append_header(&mut duplicated_launcher, LAUNCHER_HEADER, launcher);
    assert_error_code(
        session_fixture
            .manager
            .authorize_launcher(&duplicated_launcher),
        "SECURITY_DUPLICATE_HEADER",
    );
}

#[test]
fn production_configuration_accepts_only_an_exact_ipv4_loopback_origin() {
    let invalid_origins = [
        "https://127.0.0.1:43121",
        "http://127.0.0.1:0",
        "http://localhost:43121",
        "http://127.0.0.2:43121",
        "http://192.168.1.4:43121",
        "http://127.0.0.1:43121/",
        "http://127.0.0.1:43121/path",
        "http://127.0.0.1:43121?query=yes",
        "http://127.0.0.1:43121#fragment",
    ];

    for origin in invalid_origins {
        let seed = SecuritySeed::generate().expect("generate rejected seed");
        let clock = Arc::new(support::FakeSecurityClock::new());
        assert!(
            SecurityManager::from_seed(seed, origin, clock).is_err(),
            "production must reject {origin}"
        );
    }

    let valid = support::SecurityFixture::production();
    let exact = request_parts(Method::GET, Some(&valid.expected_host), None);
    valid
        .manager
        .validate_host(&exact)
        .expect("exact production Host is accepted");
    let alias = request_parts(Method::GET, Some("localhost:43121"), None);
    assert_error_code(valid.manager.validate_host(&alias), "SECURITY_INVALID_HOST");
}

#[tokio::test]
async fn development_configuration_uses_one_vite_origin_and_one_proxy_host() {
    let clock = Arc::new(support::FakeSecurityClock::new());
    let seed = SecuritySeed::generate().expect("generate development seed");
    let token = seed.initial_launch_token().clone();
    let manager = SecurityManager::from_seed_for_development(
        seed,
        "http://127.0.0.1:5173",
        "127.0.0.1:43121",
        clock,
    )
    .expect("construct explicit development security");

    let wrong_proxy = request_parts(
        Method::POST,
        Some("127.0.0.1:5173"),
        Some("http://127.0.0.1:5173"),
    );
    assert_error_code(
        RequestSecurity::exchange(&manager, &wrong_proxy, token.as_str()).await,
        "SECURITY_INVALID_HOST",
    );

    let exact = request_parts(
        Method::POST,
        Some("127.0.0.1:43121"),
        Some("http://127.0.0.1:5173"),
    );
    RequestSecurity::exchange(&manager, &exact, token.as_str())
        .await
        .expect("explicit proxy Host and Vite Origin are accepted");
    assert_eq!(
        RequestSecurity::expected_public_origin(&manager),
        "http://127.0.0.1:5173"
    );

    for (origin, proxy_host) in [
        ("http://localhost:5173", "127.0.0.1:43121"),
        ("http://192.168.1.4:5173", "127.0.0.1:43121"),
        ("http://127.0.0.1:5173/", "127.0.0.1:43121"),
        ("http://127.0.0.1:5173", "localhost:43121"),
        ("http://127.0.0.1:5173", "127.0.0.1:0"),
        ("http://127.0.0.1:5173", "127.0.0.1:43121/path"),
    ] {
        let seed = SecuritySeed::generate().expect("generate invalid development seed");
        let clock = Arc::new(support::FakeSecurityClock::new());
        assert!(
            SecurityManager::from_seed_for_development(seed, origin, proxy_host, clock).is_err(),
            "development must reject origin={origin} proxy_host={proxy_host}"
        );
    }
}

#[test]
fn launcher_authorization_requires_exact_host_and_has_one_failure_contract() {
    let fixture = support::SecurityFixture::production();
    let launcher = fixture.manager.launcher_secret().as_str();
    let exact = launcher_request(&fixture.expected_host, launcher);
    fixture
        .manager
        .authorize_launcher(&exact)
        .expect("launcher secret and exact Host authorize local routes");

    let variants = [
        "short".to_owned(),
        "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".to_owned(),
        different_secret(launcher),
    ];
    let mut fingerprint = None;
    for candidate in variants {
        let request = launcher_request(&fixture.expected_host, &candidate);
        let error = fixture
            .manager
            .authorize_launcher(&request)
            .expect_err("wrong launcher secret is rejected");
        assert_eq!(error.status, StatusCode::UNAUTHORIZED);
        assert_eq!(error.code, "SECURITY_INVALID_LAUNCHER_SECRET");
        let current = (
            error.status,
            error.code.clone(),
            error.message.clone(),
            error.retryable,
            error.details.clone(),
        );
        assert_eq!(fingerprint.get_or_insert_with(|| current.clone()), &current);
    }

    let wrong_host = launcher_request("localhost:43121", launcher);
    assert_error_code(
        fixture.manager.authorize_launcher(&wrong_host),
        "SECURITY_INVALID_HOST",
    );
}

#[tokio::test]
async fn secret_debug_output_is_redacted_and_exchange_adds_no_cors_header() {
    let fixture = support::SecurityFixture::production();
    let token = fixture.initial_launch_token.clone();
    let exchange = exchange_initial(&fixture)
        .await
        .expect("exchange for logging fixture");
    let logged_exchange = exchange.clone();
    let response = http::Response::builder()
        .header(SET_COOKIE, exchange.set_cookie.clone())
        .body(())
        .expect("construct exchange response");
    assert!(!response.headers().contains_key(ACCESS_CONTROL_ALLOW_ORIGIN));

    let session = authorize_exchange(&fixture.manager, &fixture.expected_host, exchange);
    let read = session_request(
        Method::GET,
        Some(&fixture.expected_host),
        Some(&session.cookie),
        None,
        None,
    );
    let auth =
        RequestSecurity::authorize_read(&fixture.manager, &read).expect("authorize trace fixture");
    let record = fixture
        .manager
        .session_for(&auth)
        .expect("resolve trace session");
    let exchange_request = SessionExchangeRequest {
        token: token.as_str().to_owned(),
    };
    let server_started_at = support::timestamp();
    let server_instance_id = uuid::Uuid::new_v4();
    let bootstrap = BootstrapResponse {
        csrf_token: session.csrf.clone(),
        repositories: Vec::new(),
        tasks: Vec::new(),
        latest_event_id: 0,
        server_started_at: server_started_at.into(),
        service_state: ServiceStateDto::Ready,
        service_state_generation: 0,
        max_concurrent_tasks: 4,
        scheduler: SchedulerStateDto {
            schema_version: 1,
            server_instance_id,
            server_started_at: server_started_at.into(),
            generation: 0,
            as_of_event_id: 0,
            service_state_generation: 0,
            admission_state: SchedulerAdmissionStateDto::Running,
            limits: SchedulerLimitsDto {
                global: 4,
                per_repository: 4,
                queued: 256,
                cargo_jobs_per_task: 1,
            },
            active_task_count: 0,
            queued_task_count: 0,
            queued_tasks: Vec::new(),
            stopping_tasks: Vec::new(),
            storage: SchedulerStorageDto {
                state: SchedulerStorageStateDto::Normal,
                data: SchedulerStorageScopeDto {
                    state: SchedulerStorageStateDto::Normal,
                },
                runtime: SchedulerStorageScopeDto {
                    state: SchedulerStorageStateDto::Normal,
                },
                repositories: Vec::new(),
            },
        },
    };

    let bytes = Arc::new(Mutex::new(Vec::new()));
    let writer = TraceWriter(bytes.clone());
    let subscriber = tracing_subscriber::fmt()
        .without_time()
        .with_ansi(false)
        .with_max_level(tracing::Level::INFO)
        .with_writer(move || writer.clone())
        .finish();
    let dispatch = tracing::Dispatch::new(subscriber);
    tracing::dispatcher::with_default(&dispatch, || {
        tracing::info!(
            manager = ?fixture.manager,
            launch_token = ?token,
            launcher_secret = ?fixture.manager.launcher_secret(),
            session = ?record,
            auth_context = ?auth,
            session_exchange = ?logged_exchange,
            session_exchange_request = ?exchange_request,
            bootstrap_response = ?bootstrap,
            "security redaction fixture"
        );
    });

    let output = String::from_utf8(bytes.lock().expect("lock trace bytes").clone())
        .expect("trace output is UTF-8");
    assert!(output.contains("security redaction fixture"));
    for secret in [
        token.as_str(),
        fixture.manager.launcher_secret().as_str(),
        cookie_value(&session.cookie),
        &session.csrf,
    ] {
        assert!(!output.contains(secret), "info log leaked a process secret");
    }
}

#[tokio::test]
async fn exchange_marks_the_set_cookie_header_as_sensitive() {
    let fixture = support::SecurityFixture::production();
    let exchange = exchange_initial(&fixture)
        .await
        .expect("exchange for sensitive-header fixture");

    assert!(
        exchange.set_cookie.is_sensitive(),
        "generic HTTP diagnostics must redact the session cookie"
    );
}

struct BrowserSession {
    cookie: String,
    csrf: String,
    set_cookie: String,
}

async fn exchange_initial(fixture: &support::SecurityFixture) -> Result<SessionExchange, ApiError> {
    exchange_token(fixture, fixture.initial_launch_token.as_str()).await
}

async fn exchange_token(
    fixture: &support::SecurityFixture,
    token: &str,
) -> Result<SessionExchange, ApiError> {
    let parts = request_parts(
        Method::POST,
        Some(&fixture.expected_host),
        Some(&fixture.public_origin),
    );
    RequestSecurity::exchange(&fixture.manager, &parts, token).await
}

async fn establish_initial_session(fixture: &support::SecurityFixture) -> BrowserSession {
    let exchange = exchange_initial(fixture)
        .await
        .expect("exchange initial launch token");
    authorize_exchange(&fixture.manager, &fixture.expected_host, exchange)
}

fn authorize_exchange(
    manager: &SecurityManager,
    expected_host: &str,
    exchange: SessionExchange,
) -> BrowserSession {
    let set_cookie = exchange
        .set_cookie
        .to_str()
        .expect("set-cookie is visible ASCII")
        .to_owned();
    let cookie = set_cookie
        .split(';')
        .next()
        .expect("set-cookie has a cookie pair")
        .to_owned();
    let read = session_request(Method::GET, Some(expected_host), Some(&cookie), None, None);
    let auth: AuthContext =
        RequestSecurity::authorize_read(manager, &read).expect("new cookie authorizes a read");
    let csrf = manager
        .csrf_for_auth(&auth)
        .expect("new session has a CSRF token");
    BrowserSession {
        cookie,
        csrf,
        set_cookie,
    }
}

fn session_request(
    method: Method,
    host: Option<&str>,
    cookie: Option<&str>,
    origin: Option<&str>,
    csrf: Option<&str>,
) -> http::request::Parts {
    let mut request = Request::builder().method(method).uri("/api/test");
    if let Some(host) = host {
        request = request.header(HOST, host);
    }
    if let Some(cookie) = cookie {
        request = request.header(COOKIE, cookie);
    }
    if let Some(origin) = origin {
        request = request.header(ORIGIN, origin);
    }
    if let Some(csrf) = csrf {
        request = request.header(CSRF_HEADER, csrf);
    }
    request
        .body(())
        .expect("construct session request")
        .into_parts()
        .0
}

fn launcher_request(host: &str, launcher_secret: &str) -> http::request::Parts {
    let mut parts = request_parts(Method::GET, Some(host), None);
    append_header(&mut parts, LAUNCHER_HEADER, launcher_secret);
    parts
}

fn append_header(parts: &mut http::request::Parts, name: &str, value: &str) {
    let name = HeaderName::from_bytes(name.as_bytes()).expect("valid test header name");
    let value = HeaderValue::from_str(value).expect("valid test header value");
    parts.headers.append(name, value);
}

fn assert_error_code<T>(result: Result<T, ApiError>, expected: &str) {
    match result {
        Ok(_) => panic!("expected security rejection {expected}"),
        Err(error) => assert_eq!(error.code, expected),
    }
}

fn cookie_value(cookie: &str) -> &str {
    cookie
        .strip_prefix("coding_agent_session=")
        .expect("session cookie name")
}

fn different_secret(secret: &str) -> String {
    let mut bytes = secret.as_bytes().to_vec();
    bytes[0] = if bytes[0] == b'A' { b'B' } else { b'A' };
    String::from_utf8(bytes).expect("URL-safe token remains UTF-8")
}

#[derive(Clone)]
struct TraceWriter(Arc<Mutex<Vec<u8>>>);

impl Write for TraceWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0
            .lock()
            .expect("lock trace output")
            .extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn request_parts(method: Method, host: Option<&str>, origin: Option<&str>) -> http::request::Parts {
    let mut request = Request::builder()
        .method(method)
        .uri("/api/session/exchange");
    if let Some(host) = host {
        request = request.header(HOST, host);
    }
    if let Some(origin) = origin {
        request = request.header(ORIGIN, origin);
    }
    request
        .body(())
        .expect("construct security request")
        .into_parts()
        .0
}
