use anyhow::{Context as _, bail};
use bytes::Bytes;
use concepts::storage::http_client_trace::{HttpClientTrace, RequestTrace, ResponseTrace};
use http_body_util::{BodyExt as _, Full};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use wasm_workers::http_hooks::is_forbidden_header;
use wasm_workers::http_request_policy::HttpRequestPolicy;

const MAX_REQUEST: usize = 1024 * 1024;
const MAX_RESPONSE: usize = 256 * 1024 * 1024;
const RESPONSE_CHUNK: usize = 256 * 1024;

#[derive(Deserialize)]
struct BridgeRequest {
    method: String,
    url: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

#[derive(Serialize)]
struct BridgeResponse {
    status: u16,
    headers: Vec<(String, String)>,
    body_prefix: Option<String>,
    error: Option<String>,
}

#[derive(Serialize)]
struct BridgeDone {
    body_length: usize,
    chunks: usize,
    error: Option<String>,
}

pub async fn serve(
    queue: PathBuf,
    policy: HttpRequestPolicy,
    traces: Arc<Mutex<Vec<HttpClientTrace>>>,
) -> anyhow::Result<()> {
    let policy = Arc::new(policy);
    loop {
        let mut entries = tokio::fs::read_dir(&queue).await?;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.extension().and_then(|part| part.to_str()) != Some("request") {
                continue;
            }
            let claimed = path.with_extension("working");
            if tokio::fs::rename(&path, &claimed).await.is_err() {
                continue;
            }
            let policy = policy.clone();
            let traces = traces.clone();
            tokio::spawn(async move {
                if let Err(error) = process(&claimed, &policy, &traces).await {
                    eprintln!("VM HTTP request {} failed: {error:#}", claimed.display());
                }
            });
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

async fn process(
    path: &Path,
    policy: &HttpRequestPolicy,
    traces: &Mutex<Vec<HttpClientTrace>>,
) -> anyhow::Result<()> {
    let response_path = path.with_extension("response");
    let temporary = path.with_extension("response.tmp");
    let bytes = tokio::fs::read(path).await?;
    let request: BridgeRequest = serde_json::from_slice(&bytes)?;
    let request_trace = RequestTrace {
        sent_at: chrono::Utc::now(),
        uri: request.url.clone(),
        method: request.method.clone(),
    };
    let result = execute(request, policy, path, &response_path, &temporary).await;
    traces
        .lock()
        .expect("trace mutex poisoned")
        .push(HttpClientTrace {
            req: request_trace,
            resp: Some(ResponseTrace {
                finished_at: chrono::Utc::now(),
                status: result
                    .as_ref()
                    .copied()
                    .map_err(std::string::ToString::to_string),
            }),
        });
    if let Err(error) = result {
        publish_error(&response_path, &temporary, &format!("{error:#}")).await?;
    }
    let _ = tokio::fs::remove_file(path).await;
    Ok(())
}

async fn execute(
    request: BridgeRequest,
    policy: &HttpRequestPolicy,
    request_path: &Path,
    response_path: &Path,
    response_temporary: &Path,
) -> anyhow::Result<u16> {
    if request.body.len() > MAX_REQUEST {
        bail!("request body exceeds {MAX_REQUEST} bytes");
    }
    let mut builder = hyper::Request::builder()
        .method(request.method.as_str())
        .uri(&request.url);
    // The VM guest is a real HTTP client (curl) behind a forwarding proxy, so it always
    // emits Host and may emit hop-by-hop headers. Strip forbidden headers rather than
    // rejecting (reqwest re-derives Host from the URL); the wire result matches wasmtime.
    for (name, value) in request.headers {
        if !is_forbidden_header(&name) {
            builder = builder.header(name, value);
        }
    }
    let body = Full::new(Bytes::from(request.body))
        .map_err(|never| match never {})
        .boxed_unsync();
    let mut request = builder.body(body)?;
    policy.apply(&mut request)?;
    policy.apply_body_replacement(&mut request).await;

    let (parts, body) = request.into_parts();
    let body = body.collect().await?.to_bytes();
    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let tls = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let client = reqwest::Client::builder()
        .use_preconfigured_tls(tls)
        .build()?;
    let mut outgoing = client.request(parts.method, parts.uri.to_string());
    for (name, value) in &parts.headers {
        outgoing = outgoing.header(name, value);
    }
    let response = outgoing.body(body).send().await?;
    let status = response.status().as_u16();
    let headers = response
        .headers()
        .iter()
        .filter(|(name, _)| !is_forbidden_header(name.as_str()))
        .filter_map(|(name, value)| {
            value
                .to_str()
                .ok()
                .map(|value| (name.to_string(), value.to_owned()))
        })
        .collect();
    let body = response.bytes().await?;
    if body.len() > MAX_RESPONSE {
        bail!("response exceeds {MAX_RESPONSE} bytes");
    }
    let prefix = request_path
        .file_stem()
        .and_then(|name| name.to_str())
        .context("request has no UTF-8 identifier")?;
    if !prefix
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    {
        bail!("invalid request identifier");
    }
    publish(
        response_temporary,
        response_path,
        &BridgeResponse {
            status,
            headers,
            body_prefix: Some(prefix.to_owned()),
            error: None,
        },
    )
    .await?;
    let chunks = body.len().div_ceil(RESPONSE_CHUNK);
    for (index, chunk) in body.chunks(RESPONSE_CHUNK).enumerate() {
        let final_path = request_path.with_file_name(format!("{prefix}.body-{index:08}"));
        let temporary = final_path.with_extension(format!("body-{index:08}.tmp"));
        tokio::fs::write(&temporary, chunk).await?;
        tokio::fs::rename(temporary, final_path).await?;
    }
    let done = request_path.with_file_name(format!("{prefix}.done"));
    let done_temporary = request_path.with_file_name(format!("{prefix}.done.tmp"));
    publish(
        &done_temporary,
        &done,
        &BridgeDone {
            body_length: body.len(),
            chunks,
            error: None,
        },
    )
    .await?;
    Ok(status)
}

async fn publish_error(path: &Path, temporary: &Path, error: &str) -> anyhow::Result<()> {
    publish(
        temporary,
        path,
        &BridgeResponse {
            status: 502,
            headers: Vec::new(),
            body_prefix: None,
            error: Some(error.to_owned()),
        },
    )
    .await
}

async fn publish<T: Serialize>(temporary: &Path, path: &Path, value: &T) -> anyhow::Result<()> {
    tokio::fs::write(temporary, serde_json::to_vec(value)?).await?;
    tokio::fs::rename(temporary, path).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};
    use wasm_workers::http_request_policy::{AllowedHostPolicy, HostPattern, MethodsPattern};
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{header, method, path as path_matcher},
    };

    fn policy_allowing(host: &str) -> HttpRequestPolicy {
        HttpRequestPolicy {
            hosts: vec![AllowedHostPolicy {
                pattern: HostPattern::parse_with_methods(host, MethodsPattern::AllMethods).unwrap(),
                request_url_regex: None,
                secrets: Vec::new(),
            }],
            global_allowlist: None,
            component_policy_hash: String::new(),
            server_policy_hash: String::new(),
        }
    }

    /// Drive `process` with a `.working` request file and return the parsed response.
    async fn run_bridge(url: &str, headers: Value, policy: &HttpRequestPolicy) -> Value {
        let dir = tempfile::tempdir().unwrap();
        let request_path = dir.path().join("req.working");
        let request = json!({ "method": "GET", "url": url, "headers": headers, "body": [] });
        tokio::fs::write(&request_path, serde_json::to_vec(&request).unwrap())
            .await
            .unwrap();
        let traces = Mutex::new(Vec::new());
        process(&request_path, policy, &traces).await.unwrap();
        let bytes = tokio::fs::read(request_path.with_extension("response"))
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[tokio::test]
    async fn sets_host_header_from_authority() {
        let server = MockServer::start().await;
        let real_host = format!("127.0.0.1:{}", server.address().port());
        Mock::given(method("GET"))
            .and(path_matcher("/hello"))
            .and(header("host", real_host.as_str()))
            .and(header("x-custom", "kept"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;

        let allowed = format!("http://127.0.0.1:{}", server.address().port());
        let response = run_bridge(
            &format!("{}/hello", server.uri()),
            json!([["x-custom", "kept"]]),
            &policy_allowing(&allowed),
        )
        .await;
        assert_eq!(response["status"], 200, "unexpected response: {response}");
    }

    /// The VM guest (curl) always sends Host and may send hop-by-hop headers, so the
    /// bridge strips forbidden headers instead of rejecting: the request still succeeds
    /// and the forbidden header never reaches the server.
    #[tokio::test]
    async fn strips_forbidden_request_header() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path_matcher("/hello"))
            .and(wiremock::matchers::header_exists("x-custom"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;
        // No mock accepts a forwarded `connection` header, so if it were not stripped the
        // request would 400/hang rather than match the mock above.

        let allowed = format!("http://127.0.0.1:{}", server.address().port());
        let response = run_bridge(
            &format!("{}/hello", server.uri()),
            json!([["connection", "keep-alive"], ["x-custom", "kept"]]),
            &policy_allowing(&allowed),
        )
        .await;
        assert_eq!(response["status"], 200, "unexpected response: {response}");
    }
}
