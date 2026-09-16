use anyhow::bail;
use bytes::Bytes;
use concepts::storage::http_client_trace::{HttpClientTrace, RequestTrace, ResponseTrace};
use http_body_util::{BodyExt as _, Full};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::AsyncWriteExt as _;
use wasm_workers::http_request_policy::HttpRequestPolicy;

const MAX_REQUEST: usize = 1024 * 1024;
const MAX_RESPONSE: usize = 256 * 1024 * 1024;

#[derive(Deserialize)]
struct BridgeRequest {
    id: u64,
    method: String,
    url: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

#[derive(Serialize)]
struct BridgeResponse {
    id: u64,
    status: u16,
    headers: Vec<(String, String)>,
    body_length: usize,
    error: Option<String>,
}

pub async fn serve(
    queue: PathBuf,
    policy: HttpRequestPolicy,
    traces: Arc<Mutex<Vec<HttpClientTrace>>>,
) -> anyhow::Result<()> {
    let policy = Arc::new(policy);
    let request_path = queue.join("http-request.json");
    let request_ready = queue.join("http-request-ready");
    let response_path = queue.join("http-response.json");
    let response_body = queue.join("http-response-body");
    let response_ready = queue.join("http-response-ready");
    loop {
        let Some(id) = read_ready_id(&request_ready).await else {
            tokio::time::sleep(Duration::from_millis(2)).await;
            continue;
        };
        overwrite_existing(&request_ready, b"").await?;
        if let Err(error) = process(
            id,
            &request_path,
            &response_path,
            &response_body,
            &response_ready,
            &policy,
            &traces,
        )
        .await
        {
            eprintln!("VM HTTP request {id} failed: {error:#}");
            publish_error(
                id,
                &response_path,
                &response_body,
                &response_ready,
                &format!("{error:#}"),
            )
            .await?;
        }
    }
}

async fn process(
    id: u64,
    request_path: &Path,
    response_path: &Path,
    response_body: &Path,
    response_ready: &Path,
    policy: &HttpRequestPolicy,
    traces: &Mutex<Vec<HttpClientTrace>>,
) -> anyhow::Result<()> {
    let bytes = tokio::fs::read(request_path).await?;
    let request: BridgeRequest = serde_json::from_slice(&bytes)?;
    if request.id != id {
        bail!(
            "request payload id {} does not match marker {id}",
            request.id
        );
    }
    let request_trace = RequestTrace {
        sent_at: chrono::Utc::now(),
        uri: request.url.clone(),
        method: request.method.clone(),
    };
    let result = execute(request, policy).await;
    traces
        .lock()
        .expect("trace mutex poisoned")
        .push(HttpClientTrace {
            req: request_trace,
            resp: Some(ResponseTrace {
                finished_at: chrono::Utc::now(),
                status: result
                    .as_ref()
                    .map(|response| response.status)
                    .map_err(std::string::ToString::to_string),
            }),
        });
    let response = result?;
    publish_response(id, response_path, response_body, response_ready, response).await?;
    Ok(())
}

struct ExecutedResponse {
    status: u16,
    headers: Vec<(String, String)>,
    body: Bytes,
}

async fn execute(
    request: BridgeRequest,
    policy: &HttpRequestPolicy,
) -> anyhow::Result<ExecutedResponse> {
    if request.body.len() > MAX_REQUEST {
        bail!("request body exceeds {MAX_REQUEST} bytes");
    }
    let mut builder = hyper::Request::builder()
        .method(request.method.as_str())
        .uri(&request.url);
    for (name, value) in request.headers {
        if !filtered_header(&name) {
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
        .filter(|(name, _)| !filtered_header(name.as_str()))
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
    Ok(ExecutedResponse {
        status,
        headers,
        body,
    })
}

async fn publish_response(
    id: u64,
    response_path: &Path,
    response_body: &Path,
    response_ready: &Path,
    response: ExecutedResponse,
) -> anyhow::Result<()> {
    overwrite_existing(response_body, &response.body).await?;
    overwrite_existing(
        response_path,
        &serde_json::to_vec(&BridgeResponse {
            id,
            status: response.status,
            headers: response.headers,
            body_length: response.body.len(),
            error: None,
        })?,
    )
    .await?;
    overwrite_existing(response_ready, format!("{id}\n").as_bytes()).await
}

async fn publish_error(
    id: u64,
    response_path: &Path,
    response_body: &Path,
    response_ready: &Path,
    error: &str,
) -> anyhow::Result<()> {
    overwrite_existing(response_body, b"").await?;
    overwrite_existing(
        response_path,
        &serde_json::to_vec(&BridgeResponse {
            id,
            status: 502,
            headers: Vec::new(),
            body_length: 0,
            error: Some(error.to_owned()),
        })?,
    )
    .await?;
    overwrite_existing(response_ready, format!("{id}\n").as_bytes()).await
}

async fn overwrite_existing(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(path)
        .await?;
    file.write_all(bytes).await?;
    file.flush().await?;
    Ok(())
}

async fn read_ready_id(path: &Path) -> Option<u64> {
    let bytes = tokio::fs::read(path).await.ok()?;
    let value = std::str::from_utf8(&bytes).ok()?;
    value.strip_suffix('\n')?.parse().ok()
}

fn filtered_header(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "host"
            | "connection"
            | "proxy-connection"
            | "keep-alive"
            | "transfer-encoding"
            | "te"
            | "trailer"
            | "upgrade"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyper::Method;
    use secrecy::SecretString;
    use tokio::io::AsyncReadExt as _;
    use wasm_workers::http_request_policy::{
        AllowedHostPolicy, HostPattern, HttpRequestPolicy, MethodsPattern, PlaceholderSecret,
        ReplacementLocation,
    };

    #[tokio::test]
    async fn qemu_9p_queue_applies_policy_and_returns_response() {
        const PLACEHOLDER: &str = "OBELISK_SECRET_QUEUE_TEST";
        const SECRET: &str = "swordfish";

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            loop {
                let mut buffer = [0; 4096];
                let read = socket.read(&mut buffer).await.unwrap();
                assert_ne!(read, 0, "HTTP request ended before its body arrived");
                request.extend_from_slice(&buffer[..read]);
                let Some(headers_end) = request.windows(4).position(|part| part == b"\r\n\r\n")
                else {
                    continue;
                };
                let headers_end = headers_end + 4;
                let headers = String::from_utf8_lossy(&request[..headers_end]);
                let content_length = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().unwrap())
                    })
                    .unwrap_or(0);
                if request.len() >= headers_end + content_length {
                    let body = &request[headers_end..headers_end + content_length];
                    assert!(headers.lines().any(|line| {
                        line.eq_ignore_ascii_case(&format!("x-vm-secret: {SECRET}"))
                    }));
                    assert_eq!(body, format!("body={SECRET}").as_bytes());
                    break;
                }
            }
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 9\r\nContent-Type: text/plain\r\nConnection: close\r\n\r\nbridge-ok",
                )
                .await
                .unwrap();
        });

        let queue = tempfile::tempdir().unwrap();
        let mailbox_files = [
            "http-request-ready",
            "http-request.json",
            "http-response-body",
            "http-response-ready",
            "http-response.json",
        ];
        for name in mailbox_files {
            tokio::fs::write(queue.path().join(name), b"")
                .await
                .unwrap();
        }
        let policy = HttpRequestPolicy {
            hosts: vec![AllowedHostPolicy {
                pattern: HostPattern::parse_with_methods(
                    &format!("http://127.0.0.1:{}", address.port()),
                    MethodsPattern::Specific(vec![Method::POST]),
                )
                .unwrap(),
                request_url_regex: None,
                secrets: vec![PlaceholderSecret {
                    name: "VM_SECRET".to_owned(),
                    placeholder: PLACEHOLDER.to_owned(),
                    real_value: SecretString::from(SECRET),
                    replace_in: [ReplacementLocation::Headers, ReplacementLocation::Body]
                        .into_iter()
                        .collect(),
                }],
            }],
            global_allowlist: None,
            component_policy_hash: "component-test-policy".to_owned(),
            server_policy_hash: "server-test-policy".to_owned(),
        };
        let traces = Arc::new(Mutex::new(Vec::new()));
        let bridge = tokio::spawn(serve(queue.path().to_owned(), policy, traces.clone()));

        overwrite_existing(
            &queue.path().join("http-request.json"),
            &serde_json::to_vec(&serde_json::json!({
                "id": 7,
                "method": "POST",
                "url": format!("http://{address}/anything"),
                "headers": [
                    ["content-type", "text/plain"],
                    ["x-vm-secret", PLACEHOLDER]
                ],
                "body": format!("body={PLACEHOLDER}").into_bytes()
            }))
            .unwrap(),
        )
        .await
        .unwrap();
        overwrite_existing(&queue.path().join("http-request-ready"), b"7\n")
            .await
            .unwrap();

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if read_ready_id(&queue.path().join("http-response-ready")).await == Some(7) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .expect("bridge did not publish completion");

        let response: serde_json::Value = serde_json::from_slice(
            &tokio::fs::read(queue.path().join("http-response.json"))
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(response["id"], 7);
        assert_eq!(response["status"], 200);
        assert_eq!(response["body_length"], 9);
        assert_eq!(
            tokio::fs::read(queue.path().join("http-response-body"))
                .await
                .unwrap(),
            b"bridge-ok"
        );
        let mut entries = std::fs::read_dir(queue.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect::<Vec<_>>();
        entries.sort();
        assert_eq!(entries, mailbox_files);
        assert_eq!(traces.lock().unwrap().len(), 1);

        bridge.abort();
        server.await.unwrap();
    }
}
