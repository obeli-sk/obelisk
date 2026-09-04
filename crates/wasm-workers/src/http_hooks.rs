use crate::component_logger::ComponentLogger;
use crate::http_request_policy::{HttpRequestPolicy, PolicyError, PolicyLayer};
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
    /// The deployment component name for error-message TOML snippets.
    pub(crate) component_name: String,
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
    component_name: &str,
    http_policy: &HttpRequestPolicy,
) -> Option<String> {
    if let PolicyError::RequestDenied {
        method,
        scheme,
        host,
        port,
        request_url,
        denied_by,
        ..
    } = err
    {
        let pattern = format_host_pattern(scheme, host, *port);
        let request_url_regex =
            toml_basic_string_escape(&format!("^{}$", regex::escape(request_url)));
        let server_entry = format!(
            "# server.toml (operator-owned allowlist)\n\
             [[outbound_http.allowed_host]]\n\
             pattern = \"{pattern}\"\n\
             methods = [\"{method}\"]\n\
             request_url_regex = \"{request_url_regex}\"",
            method = method.as_str()
        );
        let deployment_entry = format!(
            "# deployment.toml (component policy)\n\
             [[{config_section_hint}]]\n\
             name = \"{component_name}\"\n\
             [[{config_section_hint}.allowed_host]]\n\
             pattern = \"{pattern}\"\n\
             methods = [\"{method}\"]\n\
             request_url_regex = \"{request_url_regex}\"",
            method = method.as_str()
        );
        let entries = match denied_by {
            PolicyLayer::Component => deployment_entry,
            PolicyLayer::GlobalAllowlist => server_entry,
            PolicyLayer::Both => format!("{server_entry}\n\n{deployment_entry}"),
        };
        let entry_word = if matches!(denied_by, PolicyLayer::Both) {
            "entries"
        } else {
            "entry"
        };
        let component_policy = format_allowed_hosts(&http_policy.hosts);
        let global_allowlist = http_policy
            .global_allowlist
            .as_deref()
            .map_or_else(|| "(not enforced)".to_string(), format_allowed_hosts);
        Some(format!(
            "{err}\n\
             Effective deployment.toml component policy:\n\
             {component_policy}\n\
             Effective server.toml outbound HTTP allowlist:\n\
             {global_allowlist}\n\
             Review and add the following {entry_word} as needed.\n\n\
             {entries}"
        ))
    } else {
        None
    }
}

fn format_allowed_hosts(hosts: &[crate::http_request_policy::AllowedHostPolicy]) -> String {
    if hosts.is_empty() {
        return "(no allowed hosts)".to_string();
    }
    hosts
        .iter()
        .map(|host| match &host.request_url_regex {
            Some(regex) => format!(
                "- {}; request_url_regex = \"{}\"",
                host.pattern,
                regex.as_str()
            ),
            None => format!("- {}", host.pattern),
        })
        .collect::<Vec<_>>()
        .join("\n")
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
            let log_msg = generate_toml_snippet(
                &err,
                self.config_section_hint,
                &self.component_name,
                &self.http_policy,
            )
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
    use crate::http_request_policy::{AllowedHostPolicy, HostPattern, PolicyLayer};
    use hyper::Method;

    #[test]
    fn denial_guidance_includes_only_the_missing_server_entry() {
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
            "example-component",
            &HttpRequestPolicy::default(),
        )
        .unwrap();

        assert!(message.contains("[[outbound_http.allowed_host]]"));
        assert!(!message.contains("[[activity_wasm]]"));
        assert!(message.contains("methods = [\"POST\"]"));
        assert!(
            message.contains(
                "request_url_regex = \"^POST https://api\\\\.example\\\\.com/v1/items$\""
            ),
            "unexpected guidance: {message}"
        );
    }

    #[test]
    fn component_denial_guidance_names_the_component() {
        let message = generate_toml_snippet(
            &PolicyError::RequestDenied {
                method: Method::POST,
                scheme: "https".to_string(),
                host: "api.example.com".to_string(),
                port: 443,
                path: "/v1/items".to_string(),
                request_url: "POST https://api.example.com/v1/items".to_string(),
                denied_by: PolicyLayer::Component,
            },
            ConfigSectionHint::ActivityWasm,
            "example-component",
            &HttpRequestPolicy::default(),
        )
        .unwrap();

        assert!(!message.contains("[[outbound_http.allowed_host]]"));
        assert!(message.contains("[[activity_wasm]]\nname = \"example-component\""));
        assert!(message.contains("[[activity_wasm.allowed_host]]"));
    }

    #[test]
    fn both_denial_guidance_includes_both_entries() {
        let message = generate_toml_snippet(
            &PolicyError::RequestDenied {
                method: Method::POST,
                scheme: "https".to_string(),
                host: "api.example.com".to_string(),
                port: 443,
                path: "/v1/items".to_string(),
                request_url: "POST https://api.example.com/v1/items".to_string(),
                denied_by: PolicyLayer::Both,
            },
            ConfigSectionHint::ActivityWasm,
            "example-component",
            &HttpRequestPolicy::default(),
        )
        .unwrap();

        assert!(message.contains("[[outbound_http.allowed_host]]"));
        assert!(message.contains("[[activity_wasm.allowed_host]]"));
    }

    #[test]
    fn denial_guidance_shows_effective_allowed_hosts() {
        let policy = HttpRequestPolicy {
            hosts: vec![AllowedHostPolicy {
                pattern: HostPattern::parse_with_methods(
                    "api.example.com",
                    crate::http_request_policy::MethodsPattern::Specific(vec![Method::GET]),
                )
                .unwrap(),
                request_url_regex: Some(
                    regex::Regex::new(r"^GET https://api\.example\.com/").unwrap(),
                ),
                secrets: Vec::new(),
            }],
            global_allowlist: Some(Vec::new()),
        };
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
            "example-component",
            &policy,
        )
        .unwrap();

        assert!(message.contains("Effective deployment.toml component policy:"));
        assert!(message.contains("- https://api.example.com [GET]; request_url_regex"));
        assert!(
            message.contains("Effective server.toml outbound HTTP allowlist:\n(no allowed hosts)")
        );
    }
}
