use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use axum_extra::extract::cookie::{Cookie, SameSite};
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use coding_agent_api::{ApiError, ApiResult, AuthContext, RequestSecurity, SessionExchange};
use http::header::{COOKIE, HOST, ORIGIN};
use http::request::Parts;
use http::{HeaderName, HeaderValue, StatusCode, Uri};
use subtle::{Choice, ConstantTimeEq};

const SECRET_BYTES: usize = 32;
pub(crate) const LAUNCH_TOKEN_LIFETIME: Duration = Duration::from_secs(120);
const SESSION_COOKIE_NAME: &str = "coding_agent_session";
const CSRF_HEADER: HeaderName = HeaderName::from_static("x-csrf-token");
const LAUNCHER_SECRET_HEADER: HeaderName = HeaderName::from_static("x-launcher-secret");

pub trait SecurityClock: Send + Sync + 'static {
    fn now(&self) -> Instant;
}

#[derive(Debug, Default)]
pub struct SystemSecurityClock;

impl SecurityClock for SystemSecurityClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SecurityError {
    #[error("the operating system random source is unavailable")]
    Random(#[from] getrandom::Error),
    #[error("the public origin must be an exact nonzero IPv4 loopback HTTP origin")]
    InvalidPublicOrigin,
    #[error("the development proxy Host must be an exact nonzero IPv4 loopback authority")]
    InvalidProxyHost,
}

#[derive(Clone)]
pub struct LaunchToken(String);

impl LaunchToken {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for LaunchToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("LaunchToken")
            .field(&"<redacted>")
            .finish()
    }
}

#[derive(Clone)]
pub struct LauncherSecret(String);

impl LauncherSecret {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for LauncherSecret {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("LauncherSecret")
            .field(&"<redacted>")
            .finish()
    }
}

#[derive(Clone)]
pub struct SessionRecord {
    session_id: String,
    csrf_token: String,
}

impl SessionRecord {
    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    pub fn csrf_token(&self) -> &str {
        &self.csrf_token
    }
}

impl fmt::Debug for SessionRecord {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SessionRecord")
            .field("session_id", &"<redacted>")
            .field("csrf_token", &"<redacted>")
            .finish()
    }
}

pub struct SecuritySeed {
    initial_launch_token: LaunchToken,
    launcher_secret: LauncherSecret,
}

impl SecuritySeed {
    pub fn generate() -> Result<Self, SecurityError> {
        Ok(Self {
            initial_launch_token: LaunchToken(random_secret()?),
            launcher_secret: LauncherSecret(random_secret()?),
        })
    }

    pub fn initial_launch_token(&self) -> &LaunchToken {
        &self.initial_launch_token
    }

    pub fn launcher_secret(&self) -> &LauncherSecret {
        &self.launcher_secret
    }
}

impl fmt::Debug for SecuritySeed {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SecuritySeed")
            .field("initial_launch_token", &"<redacted>")
            .field("launcher_secret", &"<redacted>")
            .finish()
    }
}

#[derive(Clone)]
pub struct SecurityManager {
    inner: Arc<SecurityState>,
}

struct SecurityState {
    public_origin: String,
    expected_host: String,
    launcher_secret: LauncherSecret,
    clock: Arc<dyn SecurityClock>,
    launch_tokens: Mutex<HashMap<u64, LaunchTokenRecord>>,
    next_launch_token_id: AtomicU64,
    sessions: Mutex<HashMap<u64, SessionRecord>>,
    next_session_id: AtomicU64,
}

struct LaunchTokenRecord {
    token: LaunchToken,
    issued_at: Instant,
    expires_at: Instant,
}

impl fmt::Debug for SecurityManager {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SecurityManager")
            .field("public_origin", &self.inner.public_origin)
            .field("expected_host", &self.inner.expected_host)
            .finish_non_exhaustive()
    }
}

impl SecurityManager {
    pub fn from_seed(
        seed: SecuritySeed,
        public_origin: impl Into<String>,
        clock: Arc<dyn SecurityClock>,
    ) -> Result<Self, SecurityError> {
        let public_origin = public_origin.into();
        let expected_host = exact_loopback_origin_host(&public_origin)?;
        Ok(Self::from_validated_configuration(
            seed,
            public_origin,
            expected_host,
            clock,
        ))
    }

    pub fn from_seed_for_development(
        seed: SecuritySeed,
        public_origin: impl Into<String>,
        proxy_host: impl Into<String>,
        clock: Arc<dyn SecurityClock>,
    ) -> Result<Self, SecurityError> {
        let public_origin = public_origin.into();
        exact_loopback_origin_host(&public_origin)?;
        let expected_host = proxy_host.into();
        validate_loopback_authority(&expected_host)?;
        Ok(Self::from_validated_configuration(
            seed,
            public_origin,
            expected_host,
            clock,
        ))
    }

    fn from_validated_configuration(
        seed: SecuritySeed,
        public_origin: String,
        expected_host: String,
        clock: Arc<dyn SecurityClock>,
    ) -> Self {
        let issued_at = clock.now();
        let mut launch_tokens = HashMap::new();
        launch_tokens.insert(
            0,
            LaunchTokenRecord {
                token: seed.initial_launch_token,
                issued_at,
                expires_at: issued_at + LAUNCH_TOKEN_LIFETIME,
            },
        );
        Self {
            inner: Arc::new(SecurityState {
                public_origin,
                expected_host,
                launcher_secret: seed.launcher_secret,
                clock,
                launch_tokens: Mutex::new(launch_tokens),
                next_launch_token_id: AtomicU64::new(1),
                sessions: Mutex::new(HashMap::new()),
                next_session_id: AtomicU64::new(0),
            }),
        }
    }

    pub fn expected_host(&self) -> &str {
        &self.inner.expected_host
    }

    pub fn public_origin(&self) -> &str {
        &self.inner.public_origin
    }

    pub fn launcher_secret(&self) -> &LauncherSecret {
        &self.inner.launcher_secret
    }

    pub fn issue_launch_token(&self) -> Result<LaunchToken, SecurityError> {
        let token = LaunchToken(random_secret()?);
        let issued_at = self.inner.clock.now();
        let record = LaunchTokenRecord {
            token: token.clone(),
            issued_at,
            expires_at: issued_at + LAUNCH_TOKEN_LIFETIME,
        };
        let id = self
            .inner
            .next_launch_token_id
            .fetch_add(1, Ordering::Relaxed);
        lock(&self.inner.launch_tokens).insert(id, record);
        Ok(token)
    }

    pub fn launch_token_expires_at(&self, token: &LaunchToken) -> Option<Instant> {
        let now = self.inner.clock.now();
        let mut records = lock(&self.inner.launch_tokens);
        records.retain(|_, record| now < record.expires_at);
        records.values().find_map(|record| {
            let matches = secret_matches(record.token.as_str(), token.as_str());
            if matches {
                debug_assert!(record.issued_at <= record.expires_at);
                Some(record.expires_at)
            } else {
                None
            }
        })
    }

    pub fn validate_host(&self, parts: &Parts) -> ApiResult<()> {
        let host = required_header(parts, &HOST, invalid_host)?;
        if host == self.inner.expected_host {
            Ok(())
        } else {
            Err(invalid_host())
        }
    }

    pub fn authorize_launcher(&self, parts: &Parts) -> ApiResult<()> {
        self.validate_host(parts)?;
        let presented = required_header(parts, &LAUNCHER_SECRET_HEADER, invalid_launcher_secret)?;
        if secret_matches(self.inner.launcher_secret.as_str(), presented) {
            Ok(())
        } else {
            Err(invalid_launcher_secret())
        }
    }

    pub fn session_for(&self, auth: &AuthContext) -> ApiResult<SessionRecord> {
        find_session(&lock(&self.inner.sessions), &auth.session_id)
            .cloned()
            .ok_or_else(invalid_session)
    }

    pub fn csrf_for_auth(&self, auth: &AuthContext) -> ApiResult<String> {
        Ok(self.session_for(auth)?.csrf_token)
    }

    fn validate_origin(&self, parts: &Parts) -> ApiResult<()> {
        let origin = required_header(parts, &ORIGIN, invalid_origin)?;
        if origin == self.inner.public_origin {
            Ok(())
        } else {
            Err(invalid_origin())
        }
    }

    fn consume_launch_token(&self, presented: &str) -> ApiResult<()> {
        let now = self.inner.clock.now();
        let mut records = lock(&self.inner.launch_tokens);
        records.retain(|_, record| now < record.expires_at);

        let mut matched_id = None;
        for (id, record) in records.iter() {
            if secret_matches(record.token.as_str(), presented) {
                matched_id = Some(*id);
            }
        }
        let Some(id) = matched_id else {
            return Err(invalid_launch_token());
        };
        records.remove(&id);
        Ok(())
    }

    fn create_session(&self) -> ApiResult<SessionRecord> {
        let record = SessionRecord {
            session_id: random_secret().map_err(random_source_error)?,
            csrf_token: random_secret().map_err(random_source_error)?,
        };
        let id = self.inner.next_session_id.fetch_add(1, Ordering::Relaxed);
        lock(&self.inner.sessions).insert(id, record.clone());
        Ok(record)
    }

    fn authorize_session(&self, parts: &Parts) -> ApiResult<SessionRecord> {
        let cookie_header = required_header(parts, &COOKIE, invalid_session)?;
        let presented = session_cookie_value(cookie_header)?;
        find_session(&lock(&self.inner.sessions), &presented)
            .cloned()
            .ok_or_else(invalid_session)
    }
}

#[async_trait::async_trait]
impl RequestSecurity for SecurityManager {
    fn validate_host(&self, parts: &Parts) -> ApiResult<()> {
        SecurityManager::validate_host(self, parts)
    }

    async fn exchange(&self, parts: &Parts, token: &str) -> ApiResult<SessionExchange> {
        self.validate_host(parts)?;
        self.validate_origin(parts)?;
        self.consume_launch_token(token)?;
        let session = self.create_session()?;
        let cookie = Cookie::build((SESSION_COOKIE_NAME, session.session_id))
            .http_only(true)
            .same_site(SameSite::Strict)
            .path("/")
            .build();
        let mut set_cookie =
            HeaderValue::from_str(&cookie.to_string()).map_err(|_| internal_cookie_error())?;
        set_cookie.set_sensitive(true);
        Ok(SessionExchange { set_cookie })
    }

    fn authorize_read(&self, parts: &Parts) -> ApiResult<AuthContext> {
        self.validate_host(parts)?;
        let session = self.authorize_session(parts)?;
        Ok(AuthContext {
            session_id: session.session_id,
        })
    }

    fn authorize_mutation(&self, parts: &Parts) -> ApiResult<AuthContext> {
        let auth = self.authorize_read(parts)?;
        self.validate_origin(parts)?;
        let presented = required_header(parts, &CSRF_HEADER, invalid_csrf)?;
        let session = self.session_for(&auth)?;
        if secret_matches(session.csrf_token(), presented) {
            Ok(auth)
        } else {
            Err(invalid_csrf())
        }
    }

    fn expected_public_origin(&self) -> &str {
        self.public_origin()
    }
}

fn random_secret() -> Result<String, getrandom::Error> {
    let mut bytes = [0_u8; SECRET_BYTES];
    getrandom::fill(&mut bytes)?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

fn exact_loopback_origin_host(origin: &str) -> Result<String, SecurityError> {
    let uri = origin
        .parse::<Uri>()
        .map_err(|_| SecurityError::InvalidPublicOrigin)?;
    let authority = uri.authority().ok_or(SecurityError::InvalidPublicOrigin)?;
    let port = authority
        .port_u16()
        .filter(|port| *port != 0)
        .ok_or(SecurityError::InvalidPublicOrigin)?;
    let canonical_origin = format!("http://127.0.0.1:{port}");
    if uri.scheme_str() != Some("http")
        || authority.host() != "127.0.0.1"
        || origin != canonical_origin
    {
        return Err(SecurityError::InvalidPublicOrigin);
    }
    Ok(authority.as_str().to_owned())
}

fn validate_loopback_authority(authority: &str) -> Result<(), SecurityError> {
    let parsed = authority
        .parse::<http::uri::Authority>()
        .map_err(|_| SecurityError::InvalidProxyHost)?;
    let port = parsed
        .port_u16()
        .filter(|port| *port != 0)
        .ok_or(SecurityError::InvalidProxyHost)?;
    if parsed.host() != "127.0.0.1" || authority != format!("127.0.0.1:{port}") {
        return Err(SecurityError::InvalidProxyHost);
    }
    Ok(())
}

fn required_header<'a>(
    parts: &'a Parts,
    name: &HeaderName,
    missing_or_invalid: fn() -> ApiError,
) -> ApiResult<&'a str> {
    let mut values = parts.headers.get_all(name).iter();
    let first = values.next().ok_or_else(missing_or_invalid)?;
    if values.next().is_some() {
        return Err(duplicate_header());
    }
    first.to_str().map_err(|_| missing_or_invalid())
}

fn session_cookie_value(header: &str) -> ApiResult<String> {
    let mut session_value = None;
    for cookie in Cookie::split_parse(header) {
        let cookie = cookie.map_err(|_| invalid_session())?;
        if cookie.name() != SESSION_COOKIE_NAME {
            continue;
        }
        if session_value.is_some() {
            return Err(duplicate_header());
        }
        session_value = Some(cookie.value().to_owned());
    }
    session_value.ok_or_else(invalid_session)
}

fn find_session<'a>(
    sessions: &'a HashMap<u64, SessionRecord>,
    presented: &str,
) -> Option<&'a SessionRecord> {
    let mut matched = None;
    for session in sessions.values() {
        if secret_matches(session.session_id(), presented) {
            matched = Some(session);
        }
    }
    matched
}

fn secret_matches(expected: &str, presented: &str) -> bool {
    let (expected, expected_valid) = decode_secret(expected);
    let (presented, presented_valid) = decode_secret(presented);
    bool::from(expected.ct_eq(&presented) & expected_valid & presented_valid)
}

fn decode_secret(value: &str) -> ([u8; SECRET_BYTES], Choice) {
    let mut fixed = [0_u8; SECRET_BYTES];
    let Ok(decoded) = URL_SAFE_NO_PAD.decode(value) else {
        return (fixed, Choice::from(0));
    };
    if decoded.len() != SECRET_BYTES {
        return (fixed, Choice::from(0));
    }
    fixed.copy_from_slice(&decoded);
    (fixed, Choice::from(1))
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn security_error(status: StatusCode, code: &str, message: &str) -> ApiError {
    ApiError {
        status,
        code: code.to_owned(),
        message: message.to_owned(),
        retryable: false,
        details: BTreeMap::new(),
    }
}

fn duplicate_header() -> ApiError {
    security_error(
        StatusCode::BAD_REQUEST,
        "SECURITY_DUPLICATE_HEADER",
        "A security-sensitive request header was duplicated.",
    )
}

fn invalid_host() -> ApiError {
    security_error(
        StatusCode::FORBIDDEN,
        "SECURITY_INVALID_HOST",
        "The request Host is not valid for this application instance.",
    )
}

fn invalid_origin() -> ApiError {
    security_error(
        StatusCode::FORBIDDEN,
        "SECURITY_INVALID_ORIGIN",
        "The request Origin is not valid for this application instance.",
    )
}

fn invalid_launch_token() -> ApiError {
    security_error(
        StatusCode::UNAUTHORIZED,
        "SECURITY_INVALID_LAUNCH_TOKEN",
        "The one-time launch token is invalid or expired.",
    )
}

fn invalid_session() -> ApiError {
    security_error(
        StatusCode::UNAUTHORIZED,
        "SECURITY_INVALID_SESSION",
        "The process session is missing or invalid.",
    )
}

fn invalid_csrf() -> ApiError {
    security_error(
        StatusCode::FORBIDDEN,
        "SECURITY_INVALID_CSRF",
        "The CSRF token is missing or invalid.",
    )
}

fn invalid_launcher_secret() -> ApiError {
    security_error(
        StatusCode::UNAUTHORIZED,
        "SECURITY_INVALID_LAUNCHER_SECRET",
        "The launcher secret is missing or invalid.",
    )
}

fn random_source_error(_: getrandom::Error) -> ApiError {
    security_error(
        StatusCode::INTERNAL_SERVER_ERROR,
        "SECURITY_RANDOM_UNAVAILABLE",
        "Secure random generation is temporarily unavailable.",
    )
}

fn internal_cookie_error() -> ApiError {
    security_error(
        StatusCode::INTERNAL_SERVER_ERROR,
        "SECURITY_COOKIE_ENCODING_FAILED",
        "The process session cookie could not be encoded.",
    )
}
