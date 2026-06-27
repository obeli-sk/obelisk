use crate::component_logger::ComponentLogger;
use crate::http_request_policy::{HttpRequestPolicy, PolicyError};
use concepts::storage::LogLevel;
use concepts::storage::http_client_trace::{RequestTrace, ResponseTrace};
use concepts::time::ClockFn;
use tokio::sync::oneshot;
use tracing::Instrument;
use wasmtime_wasi_http::p2::body::HyperOutgoingBody;
use wasmtime_wasi_http::p2::types::{HostFutureIncomingResponse, OutgoingRequestConfig};
use wasmtime_wasi_http::p2::{HttpResult, WasiHttpHooks, default_send_request_handler};

pub type HttpClientTracesContainer = Vec<(RequestTrace, oneshot::Receiver<ResponseTrace>)>;

/// The TOML config section type for error messages.
#[derive(Clone, Copy, Debug, derive_more::Display)]
pub enum ConfigSectionHint {
    #[display("activity_js")]
    ActivityJs,
    #[display("activity_wasm")]
    ActivityWasm,
    #[display("webhook_endpoint_js")]
    WebhookEndpointJs,
    #[display("webhook_endpoint_wasm")]
    WebhookEndpointWasm,
}

pub(crate) struct HttpHooks {
    pub(crate) clock_fn: Box<dyn ClockFn>,
    pub(crate) http_client_traces: HttpClientTracesContainer,
    pub(crate) http_policy: HttpRequestPolicy,
    pub(crate) component_logger: ComponentLogger,
    /// The TOML config section type for error messages
    pub(crate) config_section_hint: ConfigSectionHint,
}

/// Generate a simplified host pattern for the TOML snippet.
/// - <https://foo:443> -> foo (HTTPS is default, 443 is default for HTTPS)
/// - <https://foo:8080> -> foo:8080 (non-default port)
/// - <http://bar:80> -> <http://bar> (HTTP is not default, but 80 is default for HTTP)
/// - <http://bar:8080> -> <http://bar:8080> (non-default port)
fn format_host_pattern(scheme: &str, host: &str, port: u16) -> String {
    match scheme {
        "https" if port == 443 => host.to_string(),
        "https" => format!("{host}:{port}"),
        "http" if port == 80 => format!("http://{host}"),
        "http" => format!("http://{host}:{port}"),
        _ => format!("{scheme}://{host}:{port}"),
    }
}

/// Generate a TOML config snippet to help users fix denied HTTP requests.
fn generate_toml_snippet(
    err: &PolicyError,
    config_section_hint: ConfigSectionHint,
) -> Option<String> {
    if let PolicyError::RequestDenied {
        method,
        scheme,
        host,
        port,
    } = err
    {
        let pattern = format_host_pattern(scheme, host, *port);
        Some(format!(
            "{err}\n\
             To allow this request, add the following to your configuration:\n\n\
             [[{section}.allowed_host]]\n\
             pattern = \"{pattern}\"\n\
             methods = [\"{method}\"]",
            section = config_section_hint,
            method = method.as_str()
        ))
    } else {
        None
    }
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
            // Generate a helpful TOML snippet for the user
            let log_msg = generate_toml_snippet(&err, self.config_section_hint)
                .unwrap_or_else(|| err.to_string());
            self.component_logger.log(LogLevel::Warn, log_msg); // Append to execution's logs table
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
