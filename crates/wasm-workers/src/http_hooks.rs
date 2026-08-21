use crate::component_logger::ComponentLogger;
use crate::http_request_policy::{HttpRequestPolicy, PolicyError};
use concepts::storage::LogLevel;
use concepts::storage::http_client_trace::{RequestTrace, ResponseTrace};
use concepts::time::ClockFn;
use http_body_util::BodyExt;
use std::future::Future;
use tokio::sync::oneshot;
use tracing::Instrument;
use wasmtime_wasi_http::p2::body::HyperOutgoingBody;
use wasmtime_wasi_http::{Error, RequestOptions, WasiHttpHooks, default_send_request};

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
        request_url,
        ..
    } = err
    {
        let pattern = format_host_pattern(scheme, host, *port);
        let request_url_regex =
            toml_basic_string_escape(&format!("^{}$", regex::escape(request_url)));
        Some(format!(
            "{err}\n\
             Review and add the following entries as needed.\n\n\
             # server.toml (operator-owned allowlist)\n\
             [[outbound_http.allowed_host]]\n\
             pattern = \"{pattern}\"\n\
             methods = [\"{method}\"]\n\
             request_url_regex = \"{request_url_regex}\"\n\n\
             # deployment.toml (component policy)\n\
             [[{section}.allowed_host]]\n\
             pattern = \"{pattern}\"\n\
             methods = [\"{method}\"]\n\
             request_url_regex = \"{request_url_regex}\"",
            section = config_section_hint,
            method = method.as_str()
        ))
    } else {
        None
    }
}

fn toml_basic_string_escape(input: &str) -> String {
    input.replace('\\', "\\\\").replace('"', "\\\"")
}

type SendRequestFuture = Box<
    dyn Future<
            Output = Result<
                (
                    hyper::Response<HyperOutgoingBody>,
                    Box<dyn Future<Output = Result<(), Error>> + Send>,
                ),
                Error,
            >,
        > + Send,
>;

impl WasiHttpHooks for HttpHooks {
    fn send_request(
        &mut self,
        mut request: hyper::Request<HyperOutgoingBody>,
        options: Option<RequestOptions>,
        _fut: Box<dyn Future<Output = Result<(), Error>> + Send>,
    ) -> SendRequestFuture {
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
            // Drain the outgoing body so the guest's request stream closes cleanly and
            // observes the denial on the response future instead of a connection reset.
            return Box::new(async move {
                let _ = request.into_body().collect().await;
                Err(Error::HttpRequestDenied)
            });
        }

        let span = tracing::info_span!(parent: &self.component_logger.span, "send_request",
            otel.name = format!("send_request {} {}", request.method(), request.uri()),
            method = %request.method(),
            uri = %request.uri(),
        );
        let clock_fn = self.clock_fn.clone_box();
        let http_policy = self.http_policy.clone();
        span.in_scope(|| tracing::debug!("Sending {request:?}"));
        Box::new(
            async move {
                http_policy.apply_body_replacement(&mut request).await;
                // workaround for https://github.com/bytecodealliance/wasmtime/issues/14190
                if !request.headers().contains_key(hyper::header::HOST)
                    && let Some(authority) = request.uri().authority()
                    && let Ok(value) = hyper::header::HeaderValue::from_str(authority.as_str())
                {
                    request.headers_mut().insert(hyper::header::HOST, value);
                }
                let resp_result = default_send_request(request, options).await;
                tracing::debug!(
                    "Got response {:?}",
                    resp_result.as_ref().map(|(resp, _io)| resp.status())
                );
                let _ = resp_trace_tx.send(ResponseTrace {
                    finished_at: clock_fn.now(),
                    status: resp_result
                        .as_ref()
                        .map(|(resp, _io)| resp.status().as_u16())
                        .map_err(std::string::ToString::to_string),
                });
                let (resp, io) = resp_result?;
                Ok((
                    resp.map(BodyExt::boxed_unsync),
                    Box::new(io) as Box<dyn Future<Output = Result<(), Error>> + Send>,
                ))
            }
            .instrument(span),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http_request_policy::PolicyLayer;
    use hyper::Method;

    #[test]
    fn denial_guidance_includes_server_and_deployment_entries() {
        let message = generate_toml_snippet(
            &PolicyError::RequestDenied {
                method: Method::POST,
                scheme: "https".to_string(),
                host: "api.example.com".to_string(),
                port: 443,
                path: "/v1/items".to_string(),
                request_url: "POST https://api.example.com/v1/items".to_string(),
                denied_by: PolicyLayer::GlobalAllowlist,
            },
            ConfigSectionHint::ActivityWasm,
        )
        .unwrap();

        assert!(message.contains("[[outbound_http.allowed_host]]"));
        assert!(message.contains("[[activity_wasm.allowed_host]]"));
        assert!(message.contains("methods = [\"POST\"]"));
        assert!(
            message.contains(
                "request_url_regex = \"^POST https://api\\\\.example\\\\.com/v1/items$\""
            ),
            "unexpected guidance: {message}"
        );
    }
}
