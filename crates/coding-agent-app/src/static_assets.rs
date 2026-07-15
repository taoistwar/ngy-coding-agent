use std::collections::HashSet;
use std::convert::Infallible;
use std::future::{Ready, ready};
use std::sync::OnceLock;
use std::task::{Context, Poll};

use axum::body::Body;
use http::header::{ACCEPT, CACHE_CONTROL, CONTENT_LENGTH, CONTENT_TYPE};
use http::{HeaderMap, Method, Request, Response, StatusCode};
#[cfg(feature = "embedded-web")]
use rust_embed::RustEmbed;
#[cfg(feature = "embedded-web")]
use serde::Deserialize;
use tower::Service;

const NO_STORE: &str = "no-store";
const IMMUTABLE: &str = "public,max-age=31536000,immutable";

#[cfg(feature = "embedded-web")]
#[derive(RustEmbed)]
#[folder = "../../web/dist/"]
struct EmbeddedWebAssets;

/// Serves the Vite production build without consulting the filesystem in release builds.
///
/// In ordinary debug builds without `embedded-web`, every request is a cache-disabled 404 so
/// development remains explicit about using the Vite proxy. The `e2e` feature enables
/// `rust-embed/debug-embed`, making debug end-to-end binaries self-contained as well.
#[derive(Clone, Copy, Debug, Default)]
pub struct StaticAssetService;

impl StaticAssetService {
    pub const fn new() -> Self {
        Self
    }

    pub async fn serve(&self, request: Request<Body>) -> Response<Body> {
        response_for(request)
    }
}

impl Service<Request<Body>> for StaticAssetService {
    type Response = Response<Body>;
    type Error = Infallible;
    type Future = Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        ready(Ok(response_for(request)))
    }
}

fn response_for(request: Request<Body>) -> Response<Body> {
    if !matches!(*request.method(), Method::GET | Method::HEAD) {
        return not_found();
    }

    let Some(path) = normalized_asset_path(request.uri().path()) else {
        return not_found();
    };
    let exact_path = if path.is_empty() { "index.html" } else { &path };
    let head_only = request.method() == Method::HEAD;

    if let Some(bytes) = embedded_asset(exact_path) {
        return asset_response(exact_path, bytes, head_only);
    }

    if !spa_fallback_allowed(&path, request.headers()) {
        return not_found();
    }

    embedded_asset("index.html")
        .map(|bytes| asset_response("index.html", bytes, head_only))
        .unwrap_or_else(not_found)
}

fn normalized_asset_path(raw_path: &str) -> Option<String> {
    let decoded = percent_decode(raw_path.as_bytes())?;
    let decoded = std::str::from_utf8(&decoded).ok()?;
    let relative = decoded.strip_prefix('/')?;

    if relative.starts_with('/')
        || relative.contains('\\')
        || relative.contains('\0')
        || relative.contains(':')
    {
        return None;
    }
    if relative
        .split('/')
        .any(|segment| segment == "." || segment == ".." || segment.starts_with('.'))
    {
        return None;
    }

    Some(relative.to_owned())
}

fn percent_decode(input: &[u8]) -> Option<Vec<u8>> {
    let mut output = Vec::with_capacity(input.len());
    let mut index = 0;
    while index < input.len() {
        if input[index] != b'%' {
            output.push(input[index]);
            index += 1;
            continue;
        }

        let high = *input.get(index + 1)?;
        let low = *input.get(index + 2)?;
        output.push((hex_value(high)? << 4) | hex_value(low)?);
        index += 3;
    }
    Some(output)
}

const fn hex_value(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

fn spa_fallback_allowed(path: &str, headers: &HeaderMap) -> bool {
    if protected_prefix(path) {
        return false;
    }

    let final_segment = path.trim_end_matches('/').rsplit('/').next().unwrap_or("");
    !final_segment.contains('.') && accepts_html(headers)
}

fn protected_prefix(path: &str) -> bool {
    path == "api" || path.starts_with("api/") || path == "_local" || path.starts_with("_local/")
}

fn accepts_html(headers: &HeaderMap) -> bool {
    let values = headers.get_all(ACCEPT);
    if values.iter().next().is_none() {
        return true;
    }

    let mut best: Option<(u8, f32)> = None;
    for value in values {
        let Ok(value) = value.to_str() else {
            continue;
        };
        for range in value.split(',') {
            let mut parts = range.split(';');
            let media_range = parts.next().unwrap_or("").trim().to_ascii_lowercase();
            let specificity = match media_range.as_str() {
                "text/html" => 2,
                "text/*" => 1,
                "*/*" => 0,
                _ => continue,
            };
            let mut quality = 1.0_f32;
            let mut quality_seen = false;
            let mut representation_matches = true;
            for parameter in parts {
                let Some((name, value)) = parameter.trim().split_once('=') else {
                    if !quality_seen {
                        representation_matches = false;
                    }
                    continue;
                };
                if !quality_seen && name.trim().eq_ignore_ascii_case("q") {
                    quality = value.trim().parse::<f32>().unwrap_or(0.0);
                    quality_seen = true;
                } else if !quality_seen {
                    // The embedded representation is plain `text/html` with no media-type
                    // parameters. Parameters before q therefore make this range non-matching;
                    // parameters after q are Accept extensions and do not affect matching.
                    representation_matches = false;
                }
            }
            if !representation_matches {
                continue;
            }
            quality = quality.clamp(0.0, 1.0);
            match best {
                Some((best_specificity, _)) if specificity < best_specificity => {}
                Some((best_specificity, best_quality)) if specificity == best_specificity => {
                    best = Some((specificity, best_quality.max(quality)));
                }
                _ => best = Some((specificity, quality)),
            }
        }
    }

    best.is_some_and(|(_, quality)| quality > 0.0)
}

fn asset_response(path: &str, bytes: Vec<u8>, head_only: bool) -> Response<Body> {
    let content_type = mime_guess::from_path(path).first_or_octet_stream();
    let cache_control = if content_type.essence_str() == "text/html" {
        NO_STORE
    } else if immutable_asset_paths().contains(path) {
        IMMUTABLE
    } else {
        NO_STORE
    };
    let content_length = bytes.len();
    let body = if head_only {
        Body::empty()
    } else {
        Body::from(bytes)
    };

    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, content_type.essence_str())
        .header(CACHE_CONTROL, cache_control)
        .header(CONTENT_LENGTH, content_length)
        .body(body)
        .expect("static response headers are valid")
}

fn not_found() -> Response<Body> {
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .header(CACHE_CONTROL, NO_STORE)
        .header(CONTENT_LENGTH, 0)
        .body(Body::empty())
        .expect("static not-found response is valid")
}

#[cfg(feature = "embedded-web")]
fn embedded_asset(path: &str) -> Option<Vec<u8>> {
    EmbeddedWebAssets::get(path).map(|asset| asset.data.into_owned())
}

#[cfg(not(feature = "embedded-web"))]
fn embedded_asset(_path: &str) -> Option<Vec<u8>> {
    None
}

#[cfg(feature = "embedded-web")]
fn immutable_asset_paths() -> &'static HashSet<String> {
    static PATHS: OnceLock<HashSet<String>> = OnceLock::new();
    PATHS.get_or_init(|| {
        let manifest = EmbeddedWebAssets::get(".vite/manifest.json")
            .expect("embedded Vite manifest is required");
        let manifest: std::collections::BTreeMap<String, ViteManifestEntry> =
            serde_json::from_slice(manifest.data.as_ref())
                .expect("embedded Vite manifest must be valid JSON");
        manifest
            .into_values()
            .flat_map(ViteManifestEntry::output_paths)
            .collect()
    })
}

#[cfg(not(feature = "embedded-web"))]
fn immutable_asset_paths() -> &'static HashSet<String> {
    static PATHS: OnceLock<HashSet<String>> = OnceLock::new();
    PATHS.get_or_init(HashSet::new)
}

#[cfg(feature = "embedded-web")]
#[derive(Deserialize)]
struct ViteManifestEntry {
    file: Option<String>,
    #[serde(default)]
    css: Vec<String>,
    #[serde(default)]
    assets: Vec<String>,
}

#[cfg(feature = "embedded-web")]
impl ViteManifestEntry {
    fn output_paths(self) -> impl Iterator<Item = String> {
        self.file.into_iter().chain(self.css).chain(self.assets)
    }
}
