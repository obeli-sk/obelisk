use crate::component_logger::ComponentLogger;
use crate::http_request_policy::{AllowedHostTomlSection, HttpRequestPolicy, PolicyError};
use concepts::storage::LogLevel;
use concepts::storage::http_client_trace::{RequestTrace, ResponseTrace};
use concepts::time::ClockFn;
use tokio::sync::oneshot;
use tracing::Instrument;
use wasmtime_wasi_http::p2::body::HyperOutgoingBody;
use wasmtime_wasi_http::p2::types::{HostFutureIncomingResponse, OutgoingRequestConfig};
use wasmtime_wasi_http::p2::{HttpResult, WasiHttpHooks, default_send_request_handler};

pub type HttpClientTracesContainer = Vec<(RequestTrace, oneshot::Receiver<ResponseTrace>)>;

pub(crate) struct HttpHooks {
    pub(crate) clock_fn: Box<dyn ClockFn>,
    pub(crate) http_client_traces: HttpClientTracesContainer,
    pub(crate) http_policy: HttpRequestPolicy,
    pub(crate) component_logger: ComponentLogger,
    pub(crate) allowed_host_toml_section: AllowedHostTomlSection,
}

impl WasiHttpHooks for HttpHooks {
    fn send_request(
        &mut self,
        mut request: hyper::Request<HyperOutgoingBody>,
        config: OutgoingRequestConfig,
    ) -> HttpResult<HostFutureIncomingResponse> {
        // Prepare request trace & channel
        let req = RequestTrace {
            sent_at: self.clock_fn.now(),
            uri: request.uri().to_string(),
            method: request.method().to_string(),
        };
        let (resp_trace_tx, resp_trace_rx) = oneshot::channel();
        self.http_client_traces.push((req, resp_trace_rx));

        // Apply HTTP policy (allowlist + placeholder replacement in headers and query params)
        let http_policy_res = self.http_policy.apply(&mut request);
        if let Err(err) = http_policy_res {
            let msg = format!(
                "{err}\n\nTo allow this request, add to your config:\n\n[[{section}.allowed_host]]\n{snippet}",
                section = self.allowed_host_toml_section,
                snippet = toml_snippet_for_denied_request(&err),
            );
            self.component_logger.log(LogLevel::Warn, msg); // Append to execution's logs table
            let _ = resp_trace_tx.send(ResponseTrace {
                finished_at: self.clock_fn.now(),
                status: Err(err.to_string()),
            });
            let err = wasmtime_wasi_http::p2::bindings::http::types::ErrorCode::from(err);
            return Err(err.into());
        }

        let span = tracing::info_span!(parent: &self.component_logger.span, "send_request",
            otel.name = format!("send_request {} {}", request.method(), request.uri()),
            method = %request.method(),
            uri = %request.uri(),
        );
        let clock_fn = self.clock_fn.clone_box();
        let http_policy = self.http_policy.clone();
        span.in_scope(|| tracing::debug!("Sending {request:?}"));
        let handle = wasmtime_wasi::runtime::spawn(
            async move {
                http_policy.apply_body_replacement(&mut request).await;
                let resp_result: Result<
                    wasmtime_wasi_http::p2::types::IncomingResponse,
                    wasmtime_wasi_http::p2::bindings::http::types::ErrorCode,
                > = default_send_request_handler(request, config).await;
                tracing::debug!("Got response {resp_result:?}");
                let _ = resp_trace_tx.send(ResponseTrace {
                    finished_at: clock_fn.now(),
                    status: resp_result
                        .as_ref()
                        .map(|resp| resp.resp.status().as_u16())
                        .map_err(std::string::ToString::to_string),
                });
                Ok(resp_result)
            }
            .instrument(span),
        );
        Ok(HostFutureIncomingResponse::pending(handle))
    }
}

/// Build a TOML snippet (`pattern` + `methods` lines) for a denied request.
fn toml_snippet_for_denied_request(err: &PolicyError) -> String {
    let PolicyError::RequestDenied {
        method,
        scheme,
        host,
        port,
    } = err
    else {
        return String::new();
    };
    // Reconstruct a concise pattern string.
    let pattern = if scheme == "https" && *port == 443 {
        host.clone()
    } else if scheme == "http" && *port == 80 {
        format!("http://{host}")
    } else {
        format!("{scheme}://{host}:{port}")
    };
    format!("pattern = \"{pattern}\"\nmethods = [\"{method}\"]")
}
