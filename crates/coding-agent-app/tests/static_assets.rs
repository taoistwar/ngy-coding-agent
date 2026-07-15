#![cfg(feature = "embedded-web")]

use std::path::{Path, PathBuf};

use axum::body::Body;
use coding_agent_app::StaticAssetService;
use http::header::{
    ACCEPT, ACCESS_CONTROL_ALLOW_ORIGIN, CACHE_CONTROL, CONTENT_LENGTH, CONTENT_TYPE,
};
use http::{Method, Request, Response, StatusCode};
use http_body_util::BodyExt;
use serde_json::Value;
use tower::ServiceExt;

const NO_STORE: &str = "no-store";
const IMMUTABLE: &str = "public,max-age=31536000,immutable";

#[tokio::test]
async fn root_serves_the_exact_built_index_without_cors() {
    let expected = std::fs::read(dist_dir().join("index.html")).expect("built index");
    let response = send(Method::GET, "/", Some("text/html")).await;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(header(&response, CONTENT_TYPE), "text/html");
    assert_eq!(header(&response, CACHE_CONTROL), NO_STORE);
    assert_eq!(
        header(&response, CONTENT_LENGTH),
        expected.len().to_string()
    );
    assert!(!response.headers().contains_key(ACCESS_CONTROL_ALLOW_ORIGIN));
    assert_eq!(body(response).await, expected);
}

#[tokio::test]
async fn manifest_listed_javascript_is_exact_and_immutable() {
    let asset = manifest_javascript();
    let expected = std::fs::read(dist_dir().join(&asset)).expect("manifest-listed JavaScript");
    let response = send(Method::GET, &format!("/{asset}"), Some("*/*")).await;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(header(&response, CONTENT_TYPE), "text/javascript");
    assert_eq!(header(&response, CACHE_CONTROL), IMMUTABLE);
    assert_eq!(
        header(&response, CONTENT_LENGTH),
        expected.len().to_string()
    );
    assert_eq!(body(response).await, expected);
}

#[tokio::test]
async fn spa_fallback_requires_a_safe_extensionless_html_navigation() {
    let expected = std::fs::read(dist_dir().join("index.html")).expect("built index");

    let response = send(
        Method::GET,
        "/repositories/example/tasks/attempt?pane=plan",
        Some("text/html,application/xhtml+xml;q=0.9"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(header(&response, CONTENT_TYPE), "text/html");
    assert_eq!(header(&response, CACHE_CONTROL), NO_STORE);
    assert_eq!(body(response).await, expected);

    for (method, path, accept) in [
        (Method::GET, "/missing.js", Some("text/html")),
        (Method::GET, "/missing", Some("application/json")),
        (
            Method::GET,
            "/repositories/profiled",
            Some("text/html;profile=\"urn:nope\";q=1, text/html;q=0"),
        ),
        (Method::POST, "/repositories/example", Some("text/html")),
        (Method::GET, "/api/not-a-route", Some("text/html")),
        (Method::GET, "/api", Some("text/html")),
        (Method::GET, "/_local/not-a-route", Some("text/html")),
        (Method::GET, "/_local", Some("text/html")),
    ] {
        let response = send(method, path, accept).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{path}");
        assert_eq!(header(&response, CACHE_CONTROL), NO_STORE, "{path}");
        assert!(body(response).await.is_empty(), "{path}");
    }
}

#[tokio::test]
async fn head_has_get_headers_and_no_body_for_assets_and_fallbacks() {
    let asset = manifest_javascript();
    for (path, accept) in [
        (format!("/{asset}"), "*/*"),
        ("/tasks/current".to_owned(), "text/html"),
    ] {
        let get = send(Method::GET, &path, Some(accept)).await;
        let head = send(Method::HEAD, &path, Some(accept)).await;

        assert_eq!(head.status(), get.status(), "{path}");
        for name in [CONTENT_TYPE, CACHE_CONTROL, CONTENT_LENGTH] {
            assert_eq!(
                head.headers().get(&name),
                get.headers().get(&name),
                "{path}"
            );
        }
        assert!(!body(get).await.is_empty(), "GET {path}");
        assert!(body(head).await.is_empty(), "HEAD {path}");
    }
}

#[tokio::test]
async fn traversal_nul_backslash_and_internal_paths_never_reach_spa_fallback() {
    for path in [
        "/../secret",
        "/%2e%2e/secret",
        "/assets/%2E%2E/secret",
        "/assets/%2e/secret",
        "/assets/%5csecret",
        "/assets/%00secret",
        "/%2Foutside-root",
        "/C:/outside-root",
        "/%43%3A/outside-root",
        "/%61pi/not-a-route",
        "/.vite/manifest.json",
        "/.hidden-route",
        "/assets/.hidden",
    ] {
        let response = send(Method::GET, path, Some("text/html")).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{path}");
        assert_eq!(header(&response, CACHE_CONTROL), NO_STORE, "{path}");
        assert!(body(response).await.is_empty(), "{path}");
    }
}

async fn send(method: Method, path: &str, accept: Option<&str>) -> Response<Body> {
    let mut builder = Request::builder().method(method).uri(path);
    if let Some(accept) = accept {
        builder = builder.header(ACCEPT, accept);
    }
    StaticAssetService::new()
        .oneshot(builder.body(Body::empty()).expect("valid request"))
        .await
        .expect("infallible static service")
}

async fn body(response: Response<Body>) -> Vec<u8> {
    response
        .into_body()
        .collect()
        .await
        .expect("read response body")
        .to_bytes()
        .to_vec()
}

fn header(response: &Response<Body>, name: http::header::HeaderName) -> String {
    response
        .headers()
        .get(name)
        .expect("required response header")
        .to_str()
        .expect("ASCII response header")
        .to_owned()
}

fn dist_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../web/dist")
}

fn manifest_javascript() -> String {
    let manifest = std::fs::read(dist_dir().join(".vite/manifest.json"))
        .expect("Vite manifest must be built before this test");
    let manifest: Value = serde_json::from_slice(&manifest).expect("valid Vite manifest");
    manifest
        .as_object()
        .expect("manifest object")
        .values()
        .filter_map(|entry| entry.get("file").and_then(Value::as_str))
        .find(|path| path.ends_with(".js"))
        .expect("manifest JavaScript output")
        .to_owned()
}
