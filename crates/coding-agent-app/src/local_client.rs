use std::fmt;
use std::net::{Ipv4Addr, SocketAddrV4};

use coding_agent_domain::UtcTimestamp;
use http::header::HOST;
use http::{Method, Request, StatusCode};
use http_body_util::{BodyExt as _, Empty};
use hyper::body::{Bytes, Incoming};
use hyper::client::conn::http1;
use hyper_util::rt::TokioIo;
use serde::Deserialize;
use serde::de::DeserializeOwned;
use tokio::net::TcpStream;
use tokio::task::JoinHandle;
use tokio::time::{Instant, timeout_at};
use uuid::Uuid;

use crate::single_instance::{RuntimeDescriptor, StartupPhase};

const READY_PATH: &str = "/_local/ready";
const REOPEN_PATH: &str = "/_local/reopen";
const LAUNCHER_SECRET_HEADER: &str = "x-launcher-secret";
const MAX_RESPONSE_BODY_BYTES: usize = 4 * 1024;

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReadyProbe {
    pub(crate) instance_id: Uuid,
    pub(crate) state: StartupPhase,
}

#[derive(Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReopenGrant {
    pub(crate) url: String,
    pub(crate) expires_at: UtcTimestamp,
}

impl fmt::Debug for ReopenGrant {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReopenGrant")
            .field("url", &"<redacted>")
            .field("expires_at", &self.expires_at)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, thiserror::Error, PartialEq, Eq)]
pub(crate) enum LocalClientError {
    #[error("local runtime request timed out")]
    Timeout,
    #[error("failed to connect to the local runtime")]
    Connect,
    #[error("failed to establish the local HTTP/1 connection")]
    Handshake,
    #[error("failed to construct the local runtime request")]
    BuildRequest,
    #[error("the local runtime HTTP request failed")]
    Request,
    #[error("the local runtime response body exceeds 4096 bytes")]
    ResponseTooLarge,
    #[error("failed to read the local runtime response")]
    ReadResponse,
    #[error("the local runtime returned invalid JSON")]
    InvalidJson,
}

pub(crate) async fn probe_ready(
    descriptor: &RuntimeDescriptor,
    deadline: Instant,
) -> Result<(StatusCode, Option<ReadyProbe>), LocalClientError> {
    request_json(descriptor, Method::GET, READY_PATH, deadline).await
}

pub(crate) async fn request_reopen(
    descriptor: &RuntimeDescriptor,
    deadline: Instant,
) -> Result<(StatusCode, Option<ReopenGrant>), LocalClientError> {
    request_json(descriptor, Method::POST, REOPEN_PATH, deadline).await
}

async fn request_json<T>(
    descriptor: &RuntimeDescriptor,
    method: Method,
    path: &'static str,
    deadline: Instant,
) -> Result<(StatusCode, Option<T>), LocalClientError>
where
    T: DeserializeOwned,
{
    timeout_at(deadline, request_json_inner(descriptor, method, path))
        .await
        .map_err(|_| LocalClientError::Timeout)?
}

async fn request_json_inner<T>(
    descriptor: &RuntimeDescriptor,
    method: Method,
    path: &'static str,
) -> Result<(StatusCode, Option<T>), LocalClientError>
where
    T: DeserializeOwned,
{
    let address = SocketAddrV4::new(Ipv4Addr::LOCALHOST, descriptor.port().get());
    let stream = TcpStream::connect(address)
        .await
        .map_err(|_| LocalClientError::Connect)?;
    let (mut sender, connection) = http1::handshake::<_, Empty<Bytes>>(TokioIo::new(stream))
        .await
        .map_err(|_| LocalClientError::Handshake)?;
    let _connection = AbortOnDrop::new(tokio::spawn(async move {
        let _ = connection.await;
    }));

    let host = format!("127.0.0.1:{}", descriptor.port());
    let request = Request::builder()
        .method(method)
        .uri(path)
        .header(HOST, host)
        .header(LAUNCHER_SECRET_HEADER, descriptor.launcher_secret())
        .body(Empty::<Bytes>::new())
        .map_err(|_| LocalClientError::BuildRequest)?;
    let response = sender
        .send_request(request)
        .await
        .map_err(|_| LocalClientError::Request)?;
    let status = response.status();
    if status != StatusCode::OK {
        return Ok((status, None));
    }

    let body = read_body(response.into_body()).await?;
    let decoded = serde_json::from_slice(&body).map_err(|_| LocalClientError::InvalidJson)?;
    Ok((status, Some(decoded)))
}

async fn read_body(mut body: Incoming) -> Result<Vec<u8>, LocalClientError> {
    let mut bytes = Vec::new();
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|_| LocalClientError::ReadResponse)?;
        let Ok(data) = frame.into_data() else {
            continue;
        };
        if data.len() > MAX_RESPONSE_BODY_BYTES.saturating_sub(bytes.len()) {
            return Err(LocalClientError::ResponseTooLarge);
        }
        bytes.extend_from_slice(&data);
    }
    Ok(bytes)
}

struct AbortOnDrop(JoinHandle<()>);

impl AbortOnDrop {
    fn new(handle: JoinHandle<()>) -> Self {
        Self(handle)
    }
}

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::num::{NonZeroU16, NonZeroU32};
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    use coding_agent_domain::UtcTimestamp;

    use super::*;
    use crate::security::SecuritySeed;

    const TIMESTAMP: &str = "2026-07-15T00:00:00Z";

    #[tokio::test]
    async fn ready_probe_uses_exact_loopback_authority_and_launcher_secret() {
        let body = format!(r#"{{"instance_id":"{}","state":"ready"}}"#, instance_id());
        let (port, request) = serve_once(StatusCode::OK, body, Duration::ZERO);
        let descriptor = descriptor(port);

        let result = probe_ready(&descriptor, Instant::now() + Duration::from_secs(2))
            .await
            .expect("probe succeeds");

        assert_eq!(result.0, StatusCode::OK);
        assert_eq!(
            result.1,
            Some(ReadyProbe {
                instance_id: instance_id(),
                state: StartupPhase::Ready,
            })
        );
        let request = request.recv().expect("server captured request");
        assert_eq!(request_line(&request), "GET /_local/ready HTTP/1.1");
        assert_eq!(
            header_values(&request, "host"),
            vec![format!("127.0.0.1:{}", descriptor.port())]
        );
        assert_eq!(
            header_values(&request, LAUNCHER_SECRET_HEADER),
            vec![descriptor.launcher_secret().to_owned()]
        );
    }

    #[tokio::test]
    async fn reopen_decodes_timestamp_without_exposing_fragment_in_debug() {
        let url = "http://127.0.0.1:4317/#token=known-secret-token";
        let body = format!(r#"{{"url":"{url}","expires_at":"{TIMESTAMP}"}}"#);
        let (port, request) = serve_once(StatusCode::OK, body, Duration::ZERO);
        let descriptor = descriptor(port);

        let (_, grant) = request_reopen(&descriptor, Instant::now() + Duration::from_secs(2))
            .await
            .expect("reopen succeeds");
        let grant = grant.expect("successful response has a grant");

        assert_eq!(grant.url, url);
        assert_eq!(
            grant.expires_at,
            UtcTimestamp::parse_rfc3339(TIMESTAMP).expect("valid expected timestamp")
        );
        assert!(!format!("{grant:?}").contains("known-secret-token"));
        let request = request.recv().expect("server captured request");
        assert_eq!(request_line(&request), "POST /_local/reopen HTTP/1.1");
    }

    #[tokio::test]
    async fn successful_response_rejects_unknown_json_fields() {
        let body = format!(
            r#"{{"instance_id":"{}","state":"ready","extra":true}}"#,
            instance_id()
        );
        let (port, _request) = serve_once(StatusCode::OK, body, Duration::ZERO);

        let error = probe_ready(&descriptor(port), Instant::now() + Duration::from_secs(2))
            .await
            .expect_err("unknown field is rejected");

        assert_eq!(error, LocalClientError::InvalidJson);
    }

    #[tokio::test]
    async fn successful_response_body_is_hard_limited() {
        let (port, _request) = serve_once(
            StatusCode::OK,
            "x".repeat(MAX_RESPONSE_BODY_BYTES + 1),
            Duration::ZERO,
        );

        let error = probe_ready(&descriptor(port), Instant::now() + Duration::from_secs(2))
            .await
            .expect_err("oversized response is rejected");

        assert_eq!(error, LocalClientError::ResponseTooLarge);
    }

    #[tokio::test]
    async fn non_success_response_is_not_decoded() {
        let (port, _request) = serve_once(
            StatusCode::SERVICE_UNAVAILABLE,
            "not JSON and intentionally ignored".to_owned(),
            Duration::ZERO,
        );

        let result = request_reopen(&descriptor(port), Instant::now() + Duration::from_secs(2))
            .await
            .expect("status response succeeds");

        assert_eq!(result, (StatusCode::SERVICE_UNAVAILABLE, None));
    }

    #[tokio::test]
    async fn one_deadline_covers_waiting_for_the_response() {
        let (port, _request) =
            serve_once(StatusCode::OK, "{}".to_owned(), Duration::from_millis(150));

        let error = probe_ready(
            &descriptor(port),
            Instant::now() + Duration::from_millis(20),
        )
        .await
        .expect_err("slow response times out");

        assert_eq!(error, LocalClientError::Timeout);
    }

    #[test]
    fn client_errors_never_render_request_secrets() {
        let descriptor = descriptor(NonZeroU16::new(1).expect("nonzero test port"));
        let sensitive_values = [descriptor.launcher_secret(), "known-browser-fragment-token"];
        let errors = [
            LocalClientError::Timeout,
            LocalClientError::Connect,
            LocalClientError::Handshake,
            LocalClientError::BuildRequest,
            LocalClientError::Request,
            LocalClientError::ResponseTooLarge,
            LocalClientError::ReadResponse,
            LocalClientError::InvalidJson,
        ];

        for error in errors {
            let display = format!("{error}");
            let debug = format!("{error:?}");
            for sensitive in sensitive_values {
                assert!(!display.contains(sensitive));
                assert!(!debug.contains(sensitive));
            }
        }
    }

    fn descriptor(port: NonZeroU16) -> RuntimeDescriptor {
        let seed = SecuritySeed::generate().expect("generate test security seed");
        RuntimeDescriptor::new(
            instance_id(),
            NonZeroU32::new(42).expect("nonzero pid"),
            port,
            UtcTimestamp::parse_rfc3339(TIMESTAMP).expect("valid timestamp"),
            seed.launcher_secret().clone(),
        )
        .expect("construct descriptor")
    }

    fn serve_once(
        status: StatusCode,
        body: String,
        delay: Duration,
    ) -> (NonZeroU16, mpsc::Receiver<String>) {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("bind test server");
        let port = NonZeroU16::new(listener.local_addr().expect("local address").port())
            .expect("listener receives a nonzero port");
        let (request_tx, request_rx) = mpsc::channel();
        thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept test connection");
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .expect("set read timeout");
            let mut request = Vec::new();
            let mut chunk = [0_u8; 1024];
            while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                let count = stream.read(&mut chunk).expect("read test request");
                if count == 0 {
                    break;
                }
                request.extend_from_slice(&chunk[..count]);
            }
            request_tx
                .send(String::from_utf8(request).expect("request is ASCII"))
                .expect("send captured request");
            thread::sleep(delay);
            let response = format!(
                "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                status.as_u16(),
                status.canonical_reason().unwrap_or("Unknown"),
                body.len(),
                body,
            );
            let _ = stream.write_all(response.as_bytes());
        });
        (port, request_rx)
    }

    fn instance_id() -> Uuid {
        Uuid::parse_str("00000000-0000-4000-8000-000000000001").expect("fixed canonical v4 UUID")
    }

    fn request_line(request: &str) -> &str {
        request.lines().next().expect("request line")
    }

    fn header_values(request: &str, wanted_name: &str) -> Vec<String> {
        request
            .lines()
            .skip(1)
            .take_while(|line| !line.is_empty())
            .filter_map(|line| line.split_once(':'))
            .filter(|(name, _)| name.eq_ignore_ascii_case(wanted_name))
            .map(|(_, value)| value.trim().to_owned())
            .collect()
    }
}
