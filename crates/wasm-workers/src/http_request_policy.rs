use hyper::Uri;
use hyper::http::Method;
use rand::RngCore;
use secrecy::{ExposeSecret, SecretString};
use std::fmt;
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
    /// The placeholder string exposed to WASM.
    pub placeholder: String,
    /// The real secret value.
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
    /// Allowed URL path prefixes. A request is allowed if its path starts with
    /// any of these prefixes. Empty list matches no URL.
    pub path_prefixes: Vec<String>,
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum HostPatternError {
    #[error("wildcard `*` must be the first or last character in host pattern: `{host}`")]
    Wildcard { host: String },
    #[error("host pattern must not contain a path: `{input}`")]
    ContainsPath { input: String },
    #[error("path prefix must start with `/`: `{prefix}`")]
    PathPrefix { prefix: String },
}

impl HostPattern {
    /// Parse a host specification string into a `HostPattern` with the given
    /// allowed `methods` and `path_prefixes`.
    ///
    /// Host rules:
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
    /// Each path prefix must start with `/`; matching is a literal string prefix
    /// (`"/foo"` matches `/foobar` and `/foo/x`, `"/foo/"` only matches `/foo/...`).
    ///
    /// # Errors
    /// Returns an error if the host wildcard is misplaced, the host contains a
    /// path, or a path prefix does not start with `/`.
    pub fn parse_with(
        input: &str,
        methods: MethodsPattern,
        path_prefixes: Vec<String>,
    ) -> Result<Self, HostPatternError> {
        if let Some(prefix) = path_prefixes.iter().find(|p| !p.starts_with('/')) {
            return Err(HostPatternError::PathPrefix {
                prefix: prefix.clone(),
            });
        }

        let (scheme, host_pattern, port) = Self::parse(input)?;

        Ok(HostPattern {
            scheme,
            host_pattern,
            port,
            methods,
            path_prefixes,
        })
    }

    fn parse(input: &str) -> Result<(SchemePattern, String, PortPattern), HostPatternError> {
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
        Ok((scheme, host, port))
    }

    /// Check if a request path is allowed by this pattern's path prefixes.
    #[must_use]
    fn path_matches(&self, path: &str) -> bool {
        self.path_prefixes
            .iter()
            .any(|prefix| path.starts_with(prefix.as_str()))
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

        // Write path prefixes (omit the default "all paths")
        if self.path_prefixes != ["/"] {
            write!(f, " path_prefixes=[{}]", self.path_prefixes.join(", "))?;
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
    pub secrets: Vec<PlaceholderSecret>,
}

/// Per-component HTTP outgoing request policy.
#[derive(Clone, Debug, Default)]
pub struct HttpRequestPolicy {
    pub hosts: Vec<AllowedHostPolicy>,
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

#[derive(Debug, thiserror::Error)]
pub(crate) enum PolicyError {
    #[error("outgoing HTTP request has no host in URI: {0}")]
    RequestHasNoHost(Uri),
    #[error("outgoing HTTP {method} request to {scheme}://{host}:{port}{path} denied")]
    RequestDenied {
        method: Method,
        scheme: String,
        host: String,
        port: u16,
        path: String,
    },
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
        let method = request.method().clone();

        // 1. Find matching host entries (check host + method + path)
        let matching: Vec<&AllowedHostPolicy> = self
            .hosts
            .iter()
            .filter(|h| {
                h.pattern.matches(&scheme, &host, port, &method) && h.pattern.path_matches(&path)
            })
            .collect();
        if matching.is_empty() {
            return Err(PolicyError::RequestDenied {
                method,
                scheme,
                host,
                port,
                path,
            });
        }

        // 2. Collect all applicable secrets from matching host entries
        let applicable: Vec<&PlaceholderSecret> =
            matching.iter().flat_map(|h| h.secrets.iter()).collect();

        if applicable.is_empty() {
            return Ok(());
        }

        // 3. Replace in header values
        let header_secrets: Vec<_> = applicable
            .iter()
            .filter(|s| s.replace_in.contains(&ReplacementLocation::Headers))
            .collect();
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

        // 4. Replace in URI query params
        let param_secrets: Vec<_> = applicable
            .iter()
            .filter(|s| s.replace_in.contains(&ReplacementLocation::Params))
            .collect();
        if !param_secrets.is_empty() {
            let uri_str = request.uri().to_string();
            let mut uri_replaced = uri_str.clone();
            for secret in &param_secrets {
                uri_replaced =
                    uri_replaced.replace(&secret.placeholder, secret.real_value.expose_secret());
            }
            if uri_replaced != uri_str
                && let Ok(new_uri) = uri_replaced.parse::<hyper::Uri>()
            {
                *request.uri_mut() = new_uri;
            }
        }

        // 5. Body replacement needs async buffering, collect applicable secrets
        //    for the caller to apply via `apply_body_replacement`.

        Ok(())
    }

    /// Get body secrets applicable for the request's target host and method.
    fn body_secrets_for(&self, uri: &hyper::Uri, method: &Method) -> Vec<&PlaceholderSecret> {
        let Some((scheme, host, port, path)) = extract_request_target(uri) else {
            return Vec::new();
        };
        self.hosts
            .iter()
            .filter(|h| {
                h.pattern.matches(&scheme, &host, port, method) && h.pattern.path_matches(&path)
            })
            .flat_map(|h| h.secrets.iter())
            .filter(|s| s.replace_in.contains(&ReplacementLocation::Body))
            .collect()
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
    /// `(env_key_for_wasm, real_value)` pairs.
    pub secret_env_mappings: Vec<(String, SecretString)>,
    /// Where in the request to perform replacement.
    pub replace_in: hashbrown::HashSet<ReplacementLocation>,
}

#[cfg(test)]
mod tests {
    use super::*;

    impl HostPattern {
        pub(crate) fn parse_with_methods_and_all_paths(
            input: &str,
            methods: MethodsPattern,
        ) -> Result<Self, HostPatternError> {
            Self::parse_with(input, methods, vec!["/".to_string()])
        }

        pub(crate) fn parse_with_all_methods_and_paths(
            input: &str,
        ) -> Result<Self, HostPatternError> {
            Self::parse_with(input, MethodsPattern::AllMethods, vec!["/".to_string()])
        }
    }

    #[test]
    fn parse_fields() {
        // (input, scheme, host_pattern, port)
        let cases = [
            (
                "api.openai.com",
                SchemePattern::Https,
                "api.openai.com",
                PortPattern::Default,
            ),
            (
                "http://localhost:8080",
                SchemePattern::Http,
                "localhost",
                PortPattern::Specific(8080),
            ),
            (
                "http://example.com",
                SchemePattern::Http,
                "example.com",
                PortPattern::Default,
            ),
            (
                "internal.corp.com:8443",
                SchemePattern::Https,
                "internal.corp.com",
                PortPattern::Specific(8443),
            ),
            ("*://*", SchemePattern::Any, "*", PortPattern::Default),
            ("*://*:*", SchemePattern::Any, "*", PortPattern::Any),
            (
                "http://localhost:*",
                SchemePattern::Http,
                "localhost",
                PortPattern::Any,
            ),
            (
                "http://192.*:*",
                SchemePattern::Http,
                "192.*",
                PortPattern::Any,
            ),
        ];
        for (input, scheme, host, port) in cases {
            let p = HostPattern::parse_with_all_methods_and_paths(input).unwrap();
            assert_eq!(p.scheme, scheme, "{input}");
            assert_eq!(p.host_pattern, host, "{input}");
            assert_eq!(p.port, port, "{input}");
        }
    }

    #[test]
    fn matches_scheme_host_port() {
        // (pattern, scheme, host, port, expected)
        let cases = [
            ("api.openai.com", "https", "api.openai.com", 443, true), // bare = https only
            ("api.openai.com", "http", "api.openai.com", 80, false),
            ("http://localhost:8080", "http", "localhost", 8080, true),
            ("http://localhost:8080", "https", "localhost", 8080, false),
            ("http://example.com", "http", "example.com", 80, true),
            ("http://example.com", "http", "example.com", 8080, false), // default port only
            ("*.example.com", "https", "api.example.com", 443, true),
            ("*.example.com", "https", "foo.bar.example.com", 443, true),
            ("*.example.com", "https", "example.com", 443, false), // no bare apex
            ("192.168.1.*", "https", "192.168.1.100", 443, true),
            ("192.168.1.*", "https", "192.168.2.100", 443, false),
            ("*", "https", "anything.com", 443, true), // all https, default port
            ("*", "http", "anything.com", 80, false),
            ("http://*", "http", "anything.com", 80, true),
            ("http://*", "https", "anything.com", 443, false),
            ("*://*", "http", "foo.com", 80, true), // any scheme, default ports only
            ("*://*", "https", "foo.com", 443, true),
            ("*://*", "http", "foo.com", 8080, false),
            ("*://*", "https", "foo.com", 8443, false),
            ("*://*:*", "http", "foo.com", 8080, true), // any scheme, any port
            ("*://*:*", "https", "foo.com", 8443, true),
            ("http://localhost:*", "http", "localhost", 3000, true), // http only, any port
            ("http://localhost:*", "https", "localhost", 443, false),
            ("http://localhost:*", "http", "other.com", 80, false),
            ("http://192.*:*", "http", "192.0.0.1", 3000, true), // wildcard host + any port
            ("http://192.*:*", "http", "10.0.0.1", 80, false),
            ("http://192.*:*", "https", "192.168.1.1", 443, false),
        ];
        for (pattern, scheme, host, port, expected) in cases {
            let p = HostPattern::parse_with_all_methods_and_paths(pattern).unwrap();
            assert_eq!(
                p.matches(scheme, host, port, &Method::GET),
                expected,
                "pattern={pattern} req={scheme}://{host}:{port}"
            );
        }
    }

    #[test]
    fn method_restriction() {
        // Specific subset: only listed methods match.
        let p = HostPattern::parse_with_methods_and_all_paths(
            "api.example.com",
            MethodsPattern::Specific(vec![Method::GET, Method::HEAD]),
        )
        .unwrap();
        assert!(p.matches("https", "api.example.com", 443, &Method::GET));
        assert!(p.matches("https", "api.example.com", 443, &Method::HEAD));
        assert!(!p.matches("https", "api.example.com", 443, &Method::POST));

        // All methods match.
        let p = HostPattern::parse_with_all_methods_and_paths("api.example.com").unwrap();
        assert_eq!(p.methods, MethodsPattern::AllMethods);
        assert!(p.matches("https", "api.example.com", 443, &Method::DELETE));

        // Empty list matches nothing.
        let p = HostPattern::parse_with_methods_and_all_paths(
            "api.example.com",
            MethodsPattern::Specific(vec![]),
        )
        .unwrap();
        assert!(!p.matches("https", "api.example.com", 443, &Method::GET));
    }

    #[test]
    fn parse_rejects_invalid() {
        // Host wildcard in the middle, or a path embedded in the host pattern.
        for input in [
            "foo.*.com",
            "http://localhost:8080/",
            "https://api.example.com/v1",
            "example.com/path",
        ] {
            assert!(
                HostPattern::parse_with_all_methods_and_paths(input).is_err(),
                "{input}"
            );
        }
        // Path prefix must start with `/`.
        let err = HostPattern::parse_with(
            "api.example.com",
            MethodsPattern::AllMethods,
            vec!["foo".to_string()],
        )
        .unwrap_err();
        assert!(matches!(err, HostPatternError::PathPrefix { .. }));
    }

    #[test]
    fn path_matches() {
        // Default is "all paths".
        let p = HostPattern::parse_with_all_methods_and_paths("api.example.com").unwrap();
        assert_eq!(p.path_prefixes, vec!["/".to_string()]);

        // (prefixes, path, expected) - matching is a literal string prefix.
        let cases: [(&[&str], &str, bool); 13] = [
            (&["/"], "/", true),
            (&["/"], "/v1/chat", true),
            (&["/foo"], "/foo", true),
            (&["/foo"], "/foobar", true), // not segment-aware
            (&["/foo"], "/foo/bar", true),
            (&["/foo"], "/bar", false),
            (&["/foo"], "/", false),
            (&["/foo/"], "/foo/bar", true),
            (&["/foo/"], "/foo", false), // bare prefix not covered by "/foo/"
            (&["/foo/"], "/foobar", false),
            (&["/v1/", "/health"], "/v1/chat", true),
            (&["/v1/", "/health"], "/healthz", true),
            (&["/v1/", "/health"], "/v2/chat", false),
        ];
        for (prefixes, path, expected) in cases {
            let p = HostPattern::parse_with(
                "api.example.com",
                MethodsPattern::AllMethods,
                prefixes.iter().map(|s| (*s).to_string()).collect(),
            )
            .unwrap();
            assert_eq!(
                p.path_matches(path),
                expected,
                "prefixes={prefixes:?} path={path}"
            );
        }
    }

    #[test]
    fn display_host_pattern() {
        // Scheme/host/port rendering (all methods + default paths are omitted).
        let host_cases = [
            ("api.openai.com", "https://api.openai.com"),
            ("http://localhost:8080", "http://localhost:8080"),
            ("internal.corp.com:8443", "https://internal.corp.com:8443"),
            ("*://*", "*://*"),
            ("*://*:*", "*://*:*"),
            ("http://localhost:*", "http://localhost:*"),
        ];
        for (input, expected) in host_cases {
            let p = HostPattern::parse_with_all_methods_and_paths(input).unwrap();
            assert_eq!(p.to_string(), expected, "{input}");
        }

        // Methods and non-default path prefixes are appended.
        let p = HostPattern::parse_with_methods_and_all_paths(
            "api.example.com",
            MethodsPattern::Specific(vec![Method::GET, Method::POST]),
        )
        .unwrap();
        assert_eq!(p.to_string(), "https://api.example.com [GET, POST]");

        let p = HostPattern::parse_with_methods_and_all_paths(
            "api.example.com",
            MethodsPattern::Specific(vec![]),
        )
        .unwrap();
        assert_eq!(p.to_string(), "https://api.example.com [NONE]");

        let p = HostPattern::parse_with(
            "api.example.com",
            MethodsPattern::AllMethods,
            vec!["/v1/".to_string(), "/health".to_string()],
        )
        .unwrap();
        assert_eq!(
            p.to_string(),
            "https://api.example.com path_prefixes=[/v1/, /health]"
        );
    }

    #[test]
    fn generate_placeholder_format_and_uniqueness() {
        let p = generate_placeholder();
        assert!(p.starts_with("OBELISK_SECRET_"));
        assert_eq!(p.len(), 15 + 64); // prefix + 64 hex chars
        assert_ne!(p, generate_placeholder());
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
