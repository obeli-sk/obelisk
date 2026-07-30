use hyper::Uri;
use hyper::http::Method;
use rand::RngCore;
use regex::Regex;
use secrecy::{ExposeSecret, SecretString};
use std::fmt;
use std::sync::Arc;
use tracing::{debug, trace};
use wasmtime_wasi_http::p2::bindings::http::types::ErrorCode;

/// Where in the outgoing request placeholders are replaced.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ReplacementLocation {
    Headers,
    Body,
    Params,
}

/// A secret with a generated placeholder, ready for injection at runtime.
/// Each execution run gets fresh placeholders.
#[derive(Clone, Debug)]
pub struct PlaceholderSecret {
    /// The logical secret name. This is the identity used to intersect a
    /// component's requested `(secret, replacement target)` pairs against the operator's
    /// global bound: the placeholder differs per execution run, but the name is
    /// stable on both sides.
    pub name: String,
    /// The placeholder string exposed to WASM. Unused on global-bound entries,
    /// which contribute authorization only and mint no placeholders.
    pub placeholder: String,
    /// The real secret value. Unused on global-bound entries.
    pub real_value: SecretString,
    /// Where in the request replacement is allowed.
    pub replace_in: hashbrown::HashSet<ReplacementLocation>,
}

/// Scheme pattern for matching requests.
#[derive(Clone, Debug, PartialEq, Eq, Hash, derive_more::Display)]
pub enum SchemePattern {
    #[display("http")]
    Http,
    #[display("https")]
    Https,
    /// Matches both http and https (used with `*://` prefix).
    #[display("*")]
    Any,
}
impl SchemePattern {
    /// Returns true if this pattern allows unencrypted HTTP traffic.
    #[must_use]
    pub fn allows_unencrypted(&self) -> bool {
        matches!(self, SchemePattern::Http | SchemePattern::Any)
    }
}

/// Port pattern for matching requests.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum PortPattern {
    /// Match a specific port.
    Specific(u16),
    /// Match any port (used with `:*` suffix).
    Any,
    /// Match the default port for the scheme (80 for http, 443 for https).
    Default,
}

/// Methods pattern for matching requests.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum MethodsPattern {
    /// All methods are allowed.
    AllMethods,
    /// Only specific methods are allowed.
    Specific(Vec<Method>),
}

/// A parsed host pattern for matching outgoing requests.
/// Supports wildcards: `*` means all hosts, `*.example.com` matches subdomains,
/// `192.168.1.*` matches a /24 range.
///
/// Special patterns:
/// - `*://*` matches any scheme (http/https), any host, but only default ports (80/443).
/// - `*://*:*` matches any scheme, any host, any port.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct HostPattern {
    pub scheme: SchemePattern,
    pub host_pattern: String,
    pub port: PortPattern,
    /// Allowed HTTP methods.
    pub methods: MethodsPattern,
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum HostPatternError {
    #[error("wildcard `*` must be the first or last character in host pattern: `{host}`")]
    Wildcard { host: String },
    #[error("host pattern must not contain a path: `{input}`")]
    ContainsPath { input: String },
}

impl HostPattern {
    /// Parse a host specification string into a `HostPattern`.
    /// Rules:
    /// - No scheme → HTTPS assumed
    /// - `*://` prefix → matches both http and https
    /// - No port → default for scheme (443 for HTTPS, 80 for HTTP)
    /// - `:*` suffix → matches any port
    /// - `*` wildcard in host must be first or last character of host portion
    ///
    /// Special patterns:
    /// - `*://*` matches any scheme, any host, default ports only (80 for http, 443 for https)
    /// - `*://*:*` matches any scheme, any host, any port
    ///
    /// # Errors
    /// Returns an error if the wildcard is in the middle of the host.
    pub fn parse_with_methods(
        input: &str,
        methods: MethodsPattern,
    ) -> Result<Self, HostPatternError> {
        let mut host_pattern = Self::parse(input)?;
        host_pattern.methods = methods;
        Ok(host_pattern)
    }

    fn parse(input: &str) -> Result<Self, HostPatternError> {
        // Check for `*://` prefix (any scheme)
        let (scheme, rest) = if let Some(rest) = input.strip_prefix("*://") {
            (SchemePattern::Any, rest)
        } else if let Some(rest) = input.strip_prefix("https://") {
            (SchemePattern::Https, rest)
        } else if let Some(rest) = input.strip_prefix("http://") {
            (SchemePattern::Http, rest)
        } else {
            (SchemePattern::Https, input)
        };

        // Reject patterns that contain a path (e.g. "http://localhost:1234/")
        if rest.contains('/') {
            return Err(HostPatternError::ContainsPath {
                input: input.to_string(),
            });
        }

        // Check for `:*` suffix (any port)
        let (host_port_str, any_port) = if let Some(stripped) = rest.strip_suffix(":*") {
            (stripped, true)
        } else {
            (rest, false)
        };

        let (host, port) = if any_port {
            (host_port_str.to_string(), PortPattern::Any)
        } else if let Some((h, p)) = host_port_str.rsplit_once(':') {
            if let Ok(port_num) = p.parse::<u16>() {
                (h.to_string(), PortPattern::Specific(port_num))
            } else {
                // Not a valid port, treat the whole thing as host
                (host_port_str.to_string(), PortPattern::Default)
            }
        } else {
            (host_port_str.to_string(), PortPattern::Default)
        };

        // Validate wildcard: must be first or last character
        if host.contains('*') && !host.starts_with('*') && !host.ends_with('*') {
            return Err(HostPatternError::Wildcard { host });
        }

        Ok(HostPattern {
            scheme,
            host_pattern: host,
            port,
            methods: MethodsPattern::AllMethods,
        })
    }

    /// Check if a (scheme, host, port, method) tuple matches this pattern.
    #[must_use]
    fn matches(&self, scheme: &str, host: &str, port: u16, method: &Method) -> bool {
        // Check scheme
        let scheme_matches = match &self.scheme {
            SchemePattern::Http => scheme == "http",
            SchemePattern::Https => scheme == "https",
            SchemePattern::Any => scheme == "http" || scheme == "https",
        };
        if !scheme_matches {
            return false;
        }

        // Check port
        let port_matches = match &self.port {
            PortPattern::Specific(p) => port == *p,
            PortPattern::Any => true,
            PortPattern::Default => {
                // For Any scheme, check if port is the default for the actual request scheme
                match scheme {
                    "http" => port == 80,
                    "https" => port == 443,
                    _ => false,
                }
            }
        };
        if !port_matches {
            return false;
        }

        // Check host pattern
        if !match_wildcard(&self.host_pattern, host) {
            return false;
        }

        // Check method
        match &self.methods {
            MethodsPattern::AllMethods => true,
            MethodsPattern::Specific(methods) => methods.contains(method),
        }
    }
}

impl fmt::Display for HostPattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Write scheme
        write!(f, "{}://{}", self.scheme, self.host_pattern)?;

        // Write port
        match &self.port {
            PortPattern::Specific(p) => write!(f, ":{p}")?,
            PortPattern::Any => write!(f, ":*")?,
            PortPattern::Default => {} // Don't show default port
        }

        // Write methods
        match &self.methods {
            MethodsPattern::AllMethods => {} // Don't show when all methods allowed
            MethodsPattern::Specific(methods) if methods.is_empty() => {
                write!(f, " [NONE]")?;
            }
            MethodsPattern::Specific(methods) => {
                let method_strs: Vec<&str> = methods.iter().map(Method::as_str).collect();
                write!(f, " [{}]", method_strs.join(", "))?;
            }
        }
        Ok(())
    }
}

/// Match a pattern with optional leading or trailing `*` wildcard.
fn match_wildcard(pattern: &str, value: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    if let Some(suffix) = pattern.strip_prefix('*') {
        return value.ends_with(suffix);
    }
    if let Some(prefix) = pattern.strip_suffix('*') {
        return value.starts_with(prefix);
    }
    pattern == value
}

/// Per-host policy entry: a host pattern with optional secrets.
#[derive(Clone, Debug)]
pub struct AllowedHostPolicy {
    pub pattern: HostPattern,
    pub request_url_regex: Option<Regex>,
    pub secrets: Vec<PlaceholderSecret>,
}

impl AllowedHostPolicy {
    fn matches_request(
        &self,
        scheme: &str,
        host: &str,
        port: u16,
        method: &Method,
        request_match: &str,
    ) -> bool {
        self.pattern.matches(scheme, host, port, method)
            && self
                .request_url_regex
                .as_ref()
                .is_none_or(|regex| regex.is_match(request_match))
    }
}

/// Per-component HTTP outgoing request policy.
///
/// Egress is gated by a symmetric two-pass intersection ([`Self::apply`]):
///
/// - `hosts` is the deployment-owned component policy (agent-authored).
/// - `global_bound` is the operator-owned outer bound from `server.toml`.
///   `None` means no operator bound is enforced (component policy alone
///   decides); `Some` means a request must additionally match the global bound,
///   and a secret is injected at a target only when that `(secret, replacement target)`
///   pair is authorized on *both* sides.
///
/// Global-bound entries carry authorization only: their `PlaceholderSecret`s
/// name the secret and its allowed replacement targets but mint no placeholder and hold no
/// real value. Injection always uses the component side's placeholder.
#[derive(Clone, Debug, Default)]
pub struct HttpRequestPolicy {
    pub hosts: Vec<AllowedHostPolicy>,
    pub global_bound: Option<Vec<AllowedHostPolicy>>,
}

/// Collect the entries in `hosts` that match the request target.
fn filter_matching<'a>(
    hosts: &'a [AllowedHostPolicy],
    scheme: &str,
    host: &str,
    port: u16,
    method: &Method,
    request_match: &str,
) -> Vec<&'a AllowedHostPolicy> {
    hosts
        .iter()
        .filter(|h| h.matches_request(scheme, host, port, method, request_match))
        .collect()
}

/// Given the component entries and (optionally) the global-bound entries that
/// matched a request, return the component-side placeholders to substitute at
/// `location`.
///
/// When a global bound is present, a component secret is kept only if a matching
/// global entry authorizes that same `(secret name, replacement target)` pair; anything
/// authorized on only one side is dropped. When no global bound is enforced, the
/// component's requested set is returned unfiltered.
fn effective_secrets<'a>(
    matching_component: &[&'a AllowedHostPolicy],
    matching_global: Option<&[&AllowedHostPolicy]>,
    location: ReplacementLocation,
) -> Vec<&'a PlaceholderSecret> {
    let requested = matching_component
        .iter()
        .flat_map(|h| h.secrets.iter())
        .filter(|s| s.replace_in.contains(&location));
    match matching_global {
        None => requested.collect(),
        Some(global) => {
            let authorized: hashbrown::HashSet<&str> = global
                .iter()
                .flat_map(|h| h.secrets.iter())
                .filter(|s| s.replace_in.contains(&location))
                .map(|s| s.name.as_str())
                .collect();
            requested
                .filter(|s| authorized.contains(s.name.as_str()))
                .collect()
        }
    }
}

/// Which content types get body replacement.
#[must_use]
pub fn is_text_content_type(content_type: &str) -> bool {
    let ct = content_type.to_ascii_lowercase();
    ct.starts_with("text/")
        || ct.starts_with("application/json")
        || ct.contains("+json")
        || ct.starts_with("application/x-www-form-urlencoded")
}

/// Extract (scheme, host, port, path) from a URI.
fn extract_request_target(uri: &hyper::Uri) -> Option<(String, String, u16, String)> {
    let scheme = uri.scheme_str().unwrap_or("https").to_string();
    let host = uri.host()?.to_string();
    let default_port = if scheme == "https" { 443 } else { 80 };
    let port = uri.port_u16().unwrap_or(default_port);
    let path = uri.path().to_string();
    Some((scheme, host, port, path))
}

fn request_match_input(uri: &hyper::Uri, method: &Method) -> Option<String> {
    let scheme = uri.scheme_str().unwrap_or("https");
    let authority = uri.authority()?.as_str();
    Some(format!("{method} {scheme}://{authority}{}", uri.path()))
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum PolicyError {
    #[error("outgoing HTTP request has no host in URI: {0}")]
    RequestHasNoHost(Uri),
    #[error("outgoing HTTP request {request_url} denied by {denied_by}")]
    RequestDenied {
        method: Method,
        scheme: String,
        host: String,
        port: u16,
        path: String,
        request_url: String,
        denied_by: PolicyLayer,
    },
}

#[derive(Clone, Copy, Debug, derive_more::Display)]
pub(crate) enum PolicyLayer {
    #[display("deployment.toml component policy")]
    Component,
    #[display("server.toml outbound HTTP bound")]
    GlobalBound,
}
impl From<PolicyError> for ErrorCode {
    fn from(_value: PolicyError) -> Self {
        ErrorCode::HttpRequestDenied
    }
}

impl HttpRequestPolicy {
    /// Check if a host is allowed and perform secret placeholder replacement in headers and query parameters.
    /// Returns the (possibly modified) request, or an error if the host is denied.
    pub(crate) fn apply(
        &self,
        request: &mut hyper::Request<wasmtime_wasi_http::p2::body::HyperOutgoingBody>,
    ) -> Result<(), PolicyError> {
        let Some((scheme, host, port, path)) = extract_request_target(request.uri()) else {
            return Err(PolicyError::RequestHasNoHost(request.uri().clone()));
        };
        let request_match = request_match_input(request.uri(), request.method())
            .ok_or_else(|| PolicyError::RequestHasNoHost(request.uri().clone()))?;
        let method = request.method().clone();

        let deny = |denied_by| PolicyError::RequestDenied {
            method: method.clone(),
            scheme: scheme.clone(),
            host: host.clone(),
            port,
            path: path.clone(),
            request_url: request_match.clone(),
            denied_by,
        };

        // 1. Pass 1 (component policy): the request must match a component entry.
        let matching: Vec<&AllowedHostPolicy> =
            filter_matching(&self.hosts, &scheme, &host, port, &method, &request_match);
        if matching.is_empty() {
            return Err(deny(PolicyLayer::Component));
        }

        // 2. Pass 2 (operator global bound): when enforced, the request must also
        //    match a global entry, or the whole request is denied regardless of
        //    any secret placement.
        let matching_global: Option<Vec<&AllowedHostPolicy>> = match &self.global_bound {
            Some(global) => {
                let m = filter_matching(global, &scheme, &host, port, &method, &request_match);
                if m.is_empty() {
                    return Err(deny(PolicyLayer::GlobalBound));
                }
                Some(m)
            }
            None => None,
        };
        let matching_global = matching_global.as_deref();

        // 3. Replace in header values, intersecting (secret, Headers) across sides.
        let header_secrets =
            effective_secrets(&matching, matching_global, ReplacementLocation::Headers);
        if !header_secrets.is_empty() {
            let headers = request.headers_mut();
            let keys: Vec<_> = headers.keys().cloned().collect();
            for key in keys {
                if let Some(val) = headers.get(&key)
                    && let Ok(val_str) = val.to_str()
                {
                    let mut replaced = val_str.to_string();
                    for secret in &header_secrets {
                        replaced = replaced
                            .replace(&secret.placeholder, secret.real_value.expose_secret());
                    }
                    if replaced != val_str
                        && let Ok(new_val) = hyper::header::HeaderValue::from_str(&replaced)
                    {
                        headers.insert(&key, new_val);
                    }
                }
            }
        }

        // 4. Replace in URI query parameter values, intersecting (secret, Params).
        let param_secrets =
            effective_secrets(&matching, matching_global, ReplacementLocation::Params);
        if !param_secrets.is_empty()
            && let Some(query) = request.uri().query()
        {
            let mut changed = false;
            let pairs = url::form_urlencoded::parse(query.as_bytes())
                .map(|(name, value)| {
                    let mut replaced = value.to_string();
                    for secret in &param_secrets {
                        replaced = replaced
                            .replace(&secret.placeholder, secret.real_value.expose_secret());
                    }
                    changed |= replaced != value.as_ref();
                    (name.into_owned(), replaced)
                })
                .collect::<Vec<_>>();
            if changed {
                let query = url::form_urlencoded::Serializer::new(String::new())
                    .extend_pairs(pairs)
                    .finish();
                let path_and_query = format!("{}?{query}", request.uri().path());
                let mut parts = request.uri().clone().into_parts();
                if let Ok(path_and_query) = path_and_query.parse() {
                    parts.path_and_query = Some(path_and_query);
                    if let Ok(uri) = hyper::Uri::from_parts(parts) {
                        *request.uri_mut() = uri;
                    }
                }
            }
        }

        // 5. Body replacement needs async buffering, collect applicable secrets
        //    for the caller to apply via `apply_body_replacement`.

        Ok(())
    }

    /// Get body secrets applicable for the request's target host and method,
    /// applying the same two-pass `(secret, Body)` intersection as [`Self::apply`].
    fn body_secrets_for(&self, uri: &hyper::Uri, method: &Method) -> Vec<&PlaceholderSecret> {
        let Some((scheme, host, port, _path)) = extract_request_target(uri) else {
            return Vec::new();
        };
        let Some(request_match) = request_match_input(uri, method) else {
            return Vec::new();
        };
        let matching = filter_matching(&self.hosts, &scheme, &host, port, method, &request_match);
        if matching.is_empty() {
            return Vec::new();
        }
        // A global bound that no entry satisfies denies the destination outright;
        // `apply` already rejected such requests before the body pass, so here we
        // simply inject nothing (deny-safe).
        let matching_global = match &self.global_bound {
            Some(global) => {
                let m = filter_matching(global, &scheme, &host, port, method, &request_match);
                if m.is_empty() {
                    return Vec::new();
                }
                Some(m)
            }
            None => None,
        };
        effective_secrets(
            &matching,
            matching_global.as_deref(),
            ReplacementLocation::Body,
        )
    }

    /// Perform async body replacement on a request.
    /// Must be called after `apply()` (which handles headers and params synchronously).
    /// Buffers the body, replaces placeholders in text content types, and re-wraps.
    pub(crate) async fn apply_body_replacement(
        &self,
        request: &mut hyper::Request<wasmtime_wasi_http::p2::body::HyperOutgoingBody>,
    ) {
        let body_secrets = self.body_secrets_for(request.uri(), request.method());
        if body_secrets.is_empty() {
            trace!("No secrets, no modifications to HTTP body");
            return;
        }

        // Only replace in text-based content types
        let should_replace = request
            .headers()
            .get(hyper::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(is_text_content_type)
            .unwrap_or(false);
        if !should_replace {
            return;
        }

        // Buffer the body
        let body = std::mem::take(request.body_mut());
        // TODO: consider chunking instead of waiting for the whole request body.
        let Ok(collected) = http_body_util::BodyExt::collect(body).await else {
            return;
        };
        let body_bytes = collected.to_bytes();
        let Ok(mut body_str) = String::from_utf8(body_bytes.to_vec()) else {
            // Not valid UTF-8, put original bytes back
            let restored =
                http_body_util::combinators::UnsyncBoxBody::new(http_body_util::BodyExt::map_err(
                    http_body_util::Full::new(body_bytes),
                    |_| unreachable!(),
                ));
            *request.body_mut() = restored;
            debug!("Not valid UTF-8, sending original HTTP body");
            return;
        };

        // Perform replacements
        for secret in &body_secrets {
            body_str = body_str.replace(&secret.placeholder, secret.real_value.expose_secret());
        }

        let new_body =
            http_body_util::combinators::UnsyncBoxBody::new(http_body_util::BodyExt::map_err(
                http_body_util::Full::new(hyper::body::Bytes::from(body_str)),
                |_| unreachable!(),
            ));
        *request.body_mut() = new_body;
        debug!("Applied secrets to HTTP body");
    }
}

/// Generate a cryptographically random placeholder string.
#[must_use]
pub fn generate_placeholder() -> String {
    let mut random_bytes = [0u8; 32];
    rand::rng().fill_bytes(&mut random_bytes);
    use std::fmt::Write;
    let hex = random_bytes
        .iter()
        .fold(String::with_capacity(64), |mut acc, b| {
            let _ = write!(acc, "{b:02x}");
            acc
        });
    format!("OBELISK_SECRET_{hex}")
}

/// Resolved per-host configuration: host pattern + optional secrets.
#[derive(Clone, Debug)]
pub struct AllowedHostConfig {
    pub pattern: HostPattern,
    pub request_url_regex: Option<Regex>,
    /// `(env_key_for_wasm, real_value)` pairs.
    pub secret_env_mappings: Vec<(String, SecretString)>,
    /// Where in the request to perform replacement.
    pub replace_in: hashbrown::HashSet<ReplacementLocation>,
}

/// Resolved operator-owned outbound HTTP configuration from `server.toml`.
///
/// This wrapper keeps the global outer bound distinct from component-owned
/// [`AllowedHostConfig`] entries when configuration is threaded into workers.
#[derive(Clone, Debug, Default)]
pub struct GlobalHttpConfig(Arc<[AllowedHostConfig]>);

impl GlobalHttpConfig {
    #[must_use]
    pub fn entries(&self) -> &[AllowedHostConfig] {
        &self.0
    }
}

impl From<Arc<[AllowedHostConfig]>> for GlobalHttpConfig {
    fn from(entries: Arc<[AllowedHostConfig]>) -> Self {
        Self(entries)
    }
}

impl From<Vec<AllowedHostConfig>> for GlobalHttpConfig {
    fn from(entries: Vec<AllowedHostConfig>) -> Self {
        Self(entries.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_body() -> wasmtime_wasi_http::p2::body::HyperOutgoingBody {
        http_body_util::combinators::UnsyncBoxBody::new(http_body_util::BodyExt::map_err(
            http_body_util::Empty::<hyper::body::Bytes>::new(),
            |_| unreachable!(),
        ))
    }

    #[test]
    fn parse_host_pattern_bare_hostname() {
        let p = HostPattern::parse("api.openai.com").unwrap();
        assert_eq!(p.scheme, SchemePattern::Https);
        assert_eq!(p.host_pattern, "api.openai.com");
        assert_eq!(p.port, PortPattern::Default);
        assert!(p.matches("https", "api.openai.com", 443, &Method::GET));
        assert!(!p.matches("http", "api.openai.com", 80, &Method::GET));
    }

    #[test]
    fn parse_host_pattern_with_scheme_and_port() {
        let p = HostPattern::parse("http://localhost:8080").unwrap();
        assert_eq!(p.scheme, SchemePattern::Http);
        assert_eq!(p.host_pattern, "localhost");
        assert_eq!(p.port, PortPattern::Specific(8080));
        assert!(p.matches("http", "localhost", 8080, &Method::GET));
        assert!(!p.matches("https", "localhost", 8080, &Method::GET));
    }

    #[test]
    fn parse_host_pattern_http_default_port() {
        let p = HostPattern::parse("http://example.com").unwrap();
        assert_eq!(p.scheme, SchemePattern::Http);
        assert_eq!(p.host_pattern, "example.com");
        assert_eq!(p.port, PortPattern::Default);
        assert!(p.matches("http", "example.com", 80, &Method::GET));
        assert!(!p.matches("http", "example.com", 8080, &Method::GET));
    }

    #[test]
    fn parse_host_pattern_wildcard_prefix() {
        let p = HostPattern::parse("*.example.com").unwrap();
        assert!(p.matches("https", "api.example.com", 443, &Method::GET));
        assert!(p.matches("https", "foo.bar.example.com", 443, &Method::POST));
        assert!(!p.matches("https", "example.com", 443, &Method::GET));
    }

    #[test]
    fn parse_host_pattern_wildcard_suffix() {
        let p = HostPattern::parse("192.168.1.*").unwrap();
        assert!(p.matches("https", "192.168.1.100", 443, &Method::GET));
        assert!(!p.matches("https", "192.168.2.100", 443, &Method::GET));
    }

    #[test]
    fn parse_host_pattern_wildcard_all_https() {
        let p = HostPattern::parse("*").unwrap();
        assert!(p.matches("https", "anything.com", 443, &Method::GET));
        assert!(!p.matches("http", "anything.com", 80, &Method::GET));
    }

    #[test]
    fn parse_host_pattern_wildcard_http() {
        let p = HostPattern::parse("http://*").unwrap();
        assert!(!p.matches("https", "anything.com", 443, &Method::GET));
        assert!(p.matches("http", "anything.com", 80, &Method::GET));
    }

    #[test]
    fn parse_host_pattern_wildcard_middle_rejected() {
        assert!(HostPattern::parse("foo.*.com").is_err());
    }

    #[test]
    fn parse_host_pattern_trailing_slash_rejected() {
        assert!(HostPattern::parse("http://localhost:8080/").is_err());
        assert!(HostPattern::parse("https://api.example.com/v1").is_err());
        assert!(HostPattern::parse("example.com/path").is_err());
    }

    #[test]
    fn parse_host_pattern_https_non_default_port() {
        let p = HostPattern::parse("internal.corp.com:8443").unwrap();
        assert_eq!(p.scheme, SchemePattern::Https);
        assert_eq!(p.host_pattern, "internal.corp.com");
        assert_eq!(p.port, PortPattern::Specific(8443));
    }

    #[test]
    fn host_pattern_method_restriction() {
        let p = HostPattern::parse_with_methods(
            "api.example.com",
            MethodsPattern::Specific(vec![Method::GET, Method::HEAD]),
        )
        .unwrap();
        assert!(p.matches("https", "api.example.com", 443, &Method::GET));
        assert!(p.matches("https", "api.example.com", 443, &Method::HEAD));
        assert!(!p.matches("https", "api.example.com", 443, &Method::POST));
        assert!(!p.matches("https", "api.example.com", 443, &Method::DELETE));
    }

    #[test]
    fn host_pattern_all_methods_allows_all() {
        let p = HostPattern::parse("api.example.com").unwrap();
        assert_eq!(p.methods, MethodsPattern::AllMethods);
        assert!(p.matches("https", "api.example.com", 443, &Method::GET));
        assert!(p.matches("https", "api.example.com", 443, &Method::POST));
        assert!(p.matches("https", "api.example.com", 443, &Method::DELETE));
        assert!(p.matches("https", "api.example.com", 443, &Method::PUT));
    }

    #[test]
    fn host_pattern_empty_methods_matches_nothing() {
        let p =
            HostPattern::parse_with_methods("api.example.com", MethodsPattern::Specific(vec![]))
                .unwrap();
        assert!(!p.matches("https", "api.example.com", 443, &Method::GET));
        assert!(!p.matches("https", "api.example.com", 443, &Method::POST));
        assert!(!p.matches("https", "api.example.com", 443, &Method::DELETE));
    }

    #[test]
    fn display_host_pattern_with_methods() {
        let p = HostPattern::parse_with_methods(
            "api.example.com",
            MethodsPattern::Specific(vec![Method::GET, Method::POST]),
        )
        .unwrap();
        assert_eq!(p.to_string(), "https://api.example.com [GET, POST]");
    }

    #[test]
    fn parse_host_pattern_any_scheme_default_ports() {
        // `*://*` matches any scheme, any host, default ports only
        let p = HostPattern::parse("*://*").unwrap();
        assert_eq!(p.scheme, SchemePattern::Any);
        assert_eq!(p.host_pattern, "*");
        assert_eq!(p.port, PortPattern::Default);

        // Should match http on port 80
        assert!(p.matches("http", "foo.com", 80, &Method::GET));
        // Should match https on port 443
        assert!(p.matches("https", "foo.com", 443, &Method::GET));
        // Should NOT match http on non-default port
        assert!(!p.matches("http", "foo.com", 8080, &Method::GET));
        // Should NOT match https on non-default port
        assert!(!p.matches("https", "foo.com", 8443, &Method::GET));
    }

    #[test]
    fn parse_host_pattern_any_scheme_any_port() {
        // `*://*:*` matches any scheme, any host, any port
        let p = HostPattern::parse("*://*:*").unwrap();
        assert_eq!(p.scheme, SchemePattern::Any);
        assert_eq!(p.host_pattern, "*");
        assert_eq!(p.port, PortPattern::Any);

        // Should match everything
        assert!(p.matches("http", "foo.com", 80, &Method::GET));
        assert!(p.matches("https", "foo.com", 443, &Method::GET));
        assert!(p.matches("http", "foo.com", 8080, &Method::GET));
        assert!(p.matches("https", "foo.com", 8443, &Method::GET));
        assert!(p.matches("http", "localhost", 3000, &Method::POST));
    }

    #[test]
    fn parse_host_pattern_any_port_specific_scheme() {
        // `http://localhost:*` matches http, localhost, any port
        let p = HostPattern::parse("http://localhost:*").unwrap();
        assert_eq!(p.scheme, SchemePattern::Http);
        assert_eq!(p.host_pattern, "localhost");
        assert_eq!(p.port, PortPattern::Any);

        assert!(p.matches("http", "localhost", 80, &Method::GET));
        assert!(p.matches("http", "localhost", 8080, &Method::GET));
        assert!(p.matches("http", "localhost", 3000, &Method::GET));
        assert!(!p.matches("https", "localhost", 443, &Method::GET));
        assert!(!p.matches("http", "other.com", 80, &Method::GET));
    }

    #[test]
    fn parse_host_pattern_wildcard_host_any_port() {
        // `http://192.*:*` matches http, any host starting with 192., any port
        let p = HostPattern::parse("http://192.*:*").unwrap();
        assert_eq!(p.scheme, SchemePattern::Http);
        assert_eq!(p.host_pattern, "192.*");
        assert_eq!(p.port, PortPattern::Any);

        assert!(p.matches("http", "192.168.1.1", 80, &Method::GET));
        assert!(p.matches("http", "192.168.1.1", 8080, &Method::GET));
        assert!(p.matches("http", "192.0.0.1", 3000, &Method::POST));
        assert!(!p.matches("https", "192.168.1.1", 443, &Method::GET));
        assert!(!p.matches("http", "10.0.0.1", 80, &Method::GET));
    }

    #[test]
    fn display_host_pattern_any_scheme() {
        let p = HostPattern::parse("*://*").unwrap();
        assert_eq!(p.to_string(), "*://*");

        let p = HostPattern::parse("*://*:*").unwrap();
        assert_eq!(p.to_string(), "*://*:*");

        let p = HostPattern::parse("http://localhost:*").unwrap();
        assert_eq!(p.to_string(), "http://localhost:*");
    }

    #[test]
    fn display_host_pattern_empty_methods() {
        let p =
            HostPattern::parse_with_methods("api.example.com", MethodsPattern::Specific(vec![]))
                .unwrap();
        assert_eq!(p.to_string(), "https://api.example.com [NONE]");
    }

    #[test]
    fn request_match_input_omits_query_params() {
        let uri: Uri = "https://api.example.com/v1/items?token=secret"
            .parse()
            .unwrap();
        assert_eq!(
            request_match_input(&uri, &Method::GET).as_deref(),
            Some("GET https://api.example.com/v1/items")
        );
    }

    #[test]
    fn request_match_input_preserves_non_default_port() {
        let uri: Uri = "https://api.example.com:8443/v1/items?token=secret"
            .parse()
            .unwrap();
        assert_eq!(
            request_match_input(&uri, &Method::GET).as_deref(),
            Some("GET https://api.example.com:8443/v1/items")
        );
    }

    #[test]
    fn request_url_regex_restricts_query_stripped_method_url() {
        let policy = AllowedHostPolicy {
            pattern: HostPattern::parse_with_methods(
                "api.example.com",
                MethodsPattern::Specific(vec![Method::GET]),
            )
            .unwrap(),
            request_url_regex: Some(Regex::new(r"^GET https://api\.example\.com/v1/").unwrap()),
            secrets: Vec::new(),
        };

        assert!(policy.matches_request(
            "https",
            "api.example.com",
            443,
            &Method::GET,
            "GET https://api.example.com/v1/items"
        ));
        assert!(!policy.matches_request(
            "https",
            "api.example.com",
            443,
            &Method::GET,
            "GET https://api.example.com/v2/items"
        ));
        assert!(!policy.matches_request(
            "https",
            "api.example.com",
            443,
            &Method::POST,
            "POST https://api.example.com/v1/items"
        ));
    }

    #[test]
    fn params_replace_only_query_parameter_values() {
        const PLACEHOLDER: &str = "OBELISK_SECRET_TEST_PLACEHOLDER";
        let policy = HttpRequestPolicy {
            hosts: vec![AllowedHostPolicy {
                pattern: HostPattern::parse("api.example.com").unwrap(),
                request_url_regex: None,
                secrets: vec![PlaceholderSecret {
                    name: "SECRET".to_string(),
                    placeholder: PLACEHOLDER.to_string(),
                    real_value: SecretString::from("real/value? with space"),
                    replace_in: hashbrown::HashSet::from([ReplacementLocation::Params]),
                }],
            }],
            global_bound: None,
        };
        let uri = format!(
            "https://api.example.com/{PLACEHOLDER}?{PLACEHOLDER}=unchanged&token=Bearer-{PLACEHOLDER}"
        );
        let mut request = hyper::Request::builder()
            .uri(uri)
            .body(empty_body())
            .unwrap();

        policy.apply(&mut request).unwrap();

        assert_eq!(request.uri().path(), format!("/{PLACEHOLDER}"));
        let params = url::form_urlencoded::parse(request.uri().query().unwrap().as_bytes())
            .into_owned()
            .collect::<Vec<_>>();
        assert_eq!(
            params,
            vec![
                (PLACEHOLDER.to_string(), "unchanged".to_string()),
                (
                    "token".to_string(),
                    "Bearer-real/value? with space".to_string()
                ),
            ]
        );
    }

    /// Two rules match the same request: rule 1 allows s1,s2 in headers, rule 2
    /// allows s2,s3 in params. A single pass must union per location: s1 in
    /// headers only, s3 in params only, s2 in both.
    #[test]
    fn overlapping_rules_union_secret_replacement_per_target() {
        let headers_only = |name: &str, ph: &str, real: &str| PlaceholderSecret {
            name: name.to_string(),
            placeholder: ph.to_string(),
            real_value: SecretString::from(real),
            replace_in: hashbrown::HashSet::from([ReplacementLocation::Headers]),
        };
        let params_only = |name: &str, ph: &str, real: &str| PlaceholderSecret {
            name: name.to_string(),
            placeholder: ph.to_string(),
            real_value: SecretString::from(real),
            replace_in: hashbrown::HashSet::from([ReplacementLocation::Params]),
        };
        let host = || AllowedHostPolicy {
            pattern: HostPattern::parse("api.example.com").unwrap(),
            request_url_regex: None,
            secrets: Vec::new(),
        };
        let policy = HttpRequestPolicy {
            hosts: vec![
                AllowedHostPolicy {
                    secrets: vec![
                        headers_only("S1", "PH_S1", "REAL_1"),
                        headers_only("S2", "PH_S2", "REAL_2"),
                    ],
                    ..host()
                },
                AllowedHostPolicy {
                    secrets: vec![
                        params_only("S2", "PH_S2", "REAL_2"),
                        params_only("S3", "PH_S3", "REAL_3"),
                    ],
                    ..host()
                },
            ],
            global_bound: None,
        };

        let mut request = hyper::Request::builder()
            .uri("https://api.example.com/path?a=PH_S1&b=PH_S2&c=PH_S3")
            .header("h-s1", "v-PH_S1")
            .header("h-s2", "v-PH_S2")
            .header("h-s3", "v-PH_S3")
            .body(empty_body())
            .unwrap();

        policy.apply(&mut request).unwrap();

        // Headers: s1 and s2 replaced, s3 (params-only) left as the placeholder.
        assert_eq!(request.headers().get("h-s1").unwrap(), "v-REAL_1");
        assert_eq!(request.headers().get("h-s2").unwrap(), "v-REAL_2");
        assert_eq!(request.headers().get("h-s3").unwrap(), "v-PH_S3");

        // Params: s2 and s3 replaced, s1 (headers-only) left as the placeholder.
        let params = url::form_urlencoded::parse(request.uri().query().unwrap().as_bytes())
            .into_owned()
            .collect::<Vec<_>>();
        assert_eq!(
            params,
            vec![
                ("a".to_string(), "PH_S1".to_string()),
                ("b".to_string(), "REAL_2".to_string()),
                ("c".to_string(), "REAL_3".to_string()),
            ]
        );
    }

    /// Double pass: the operator global bound and the component policy must both
    /// authorize a `(secret, replacement target)` pair for injection. The component asks to
    /// place S1 in headers and params and S2 in headers; the global bound
    /// authorizes only S1-in-headers. Only that one pair is substituted.
    #[test]
    fn global_bound_intersects_secret_and_replacement_target() {
        let secret =
            |name: &str, ph: &str, real: &str, locs: &[ReplacementLocation]| PlaceholderSecret {
                name: name.to_string(),
                placeholder: ph.to_string(),
                real_value: SecretString::from(real),
                replace_in: locs.iter().copied().collect(),
            };
        let host = |secrets: Vec<PlaceholderSecret>| AllowedHostPolicy {
            pattern: HostPattern::parse("api.example.com").unwrap(),
            request_url_regex: None,
            secrets,
        };
        let policy = HttpRequestPolicy {
            hosts: vec![host(vec![
                secret(
                    "S1",
                    "PH_S1",
                    "REAL_1",
                    &[ReplacementLocation::Headers, ReplacementLocation::Params],
                ),
                secret("S2", "PH_S2", "REAL_2", &[ReplacementLocation::Headers]),
            ])],
            // Global bound authorizes S1 in headers only; S2 not listed at all.
            // Placeholder/real_value are unused on the authorization side.
            global_bound: Some(vec![host(vec![secret(
                "S1",
                "",
                "",
                &[ReplacementLocation::Headers],
            )])]),
        };

        let mut request = hyper::Request::builder()
            .uri("https://api.example.com/path?a=PH_S1")
            .header("h1", "v-PH_S1")
            .header("h2", "v-PH_S2")
            .body(empty_body())
            .unwrap();

        policy.apply(&mut request).unwrap();

        // S1 in headers: authorized by both -> replaced.
        assert_eq!(request.headers().get("h1").unwrap(), "v-REAL_1");
        // S2 in headers: not in global bound -> left as placeholder.
        assert_eq!(request.headers().get("h2").unwrap(), "v-PH_S2");
        // S1 in params: component asked, but global bound withheld Params -> left.
        let params = url::form_urlencoded::parse(request.uri().query().unwrap().as_bytes())
            .into_owned()
            .collect::<Vec<_>>();
        assert_eq!(params, vec![("a".to_string(), "PH_S1".to_string())]);
    }

    /// Double pass: a destination the component allows but the global bound does
    /// not is denied outright, before any secret handling.
    #[test]
    fn global_bound_denies_destination_component_allows() {
        let host = |name: &str| AllowedHostPolicy {
            pattern: HostPattern::parse(name).unwrap(),
            request_url_regex: None,
            secrets: Vec::new(),
        };
        let policy = HttpRequestPolicy {
            hosts: vec![host("api.example.com")],
            global_bound: Some(vec![host("other.example.com")]),
        };
        let mut request = hyper::Request::builder()
            .uri("https://api.example.com/path")
            .body(empty_body())
            .unwrap();

        let err = policy.apply(&mut request).unwrap_err();
        assert!(matches!(err, PolicyError::RequestDenied { .. }));
    }

    #[test]
    fn empty_global_bound_denies_every_destination() {
        let policy = HttpRequestPolicy {
            hosts: vec![AllowedHostPolicy {
                pattern: HostPattern::parse("*://*:*").unwrap(),
                request_url_regex: None,
                secrets: Vec::new(),
            }],
            global_bound: Some(Vec::new()),
        };
        let mut request = hyper::Request::builder()
            .uri("https://api.example.com/path")
            .body(empty_body())
            .unwrap();

        let err = policy.apply(&mut request).unwrap_err();
        assert!(matches!(
            err,
            PolicyError::RequestDenied {
                denied_by: PolicyLayer::GlobalBound,
                ..
            }
        ));
    }

    #[test]
    fn generate_placeholder_format() {
        let p = generate_placeholder();
        assert!(p.starts_with("OBELISK_SECRET_"));
        assert_eq!(p.len(), 15 + 64); // prefix + 64 hex chars
    }

    #[test]
    fn generate_placeholder_unique() {
        let p1 = generate_placeholder();
        let p2 = generate_placeholder();
        assert_ne!(p1, p2);
    }

    #[test]
    fn display_host_pattern() {
        let p = HostPattern::parse("api.openai.com").unwrap();
        assert_eq!(p.to_string(), "https://api.openai.com");

        let p = HostPattern::parse("http://localhost:8080").unwrap();
        assert_eq!(p.to_string(), "http://localhost:8080");

        let p = HostPattern::parse("internal.corp.com:8443").unwrap();
        assert_eq!(p.to_string(), "https://internal.corp.com:8443");
    }

    #[test]
    fn test_is_text_content_type() {
        assert!(is_text_content_type("application/json"));
        assert!(is_text_content_type("application/json; charset=utf-8"));
        assert!(is_text_content_type("application/vnd.api+json"));
        assert!(is_text_content_type("text/plain"));
        assert!(is_text_content_type("text/html"));
        assert!(is_text_content_type("application/x-www-form-urlencoded"));
        assert!(!is_text_content_type("application/octet-stream"));
        assert!(!is_text_content_type("image/png"));
    }
}
