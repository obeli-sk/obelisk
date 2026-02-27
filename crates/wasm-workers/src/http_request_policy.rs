use hyper::Uri;
use hyper::http::Method;
use rand::RngCore;
use secrecy::{ExposeSecret, SecretString};
use std::fmt;
use tracing::{debug, trace};
use wasmtime_wasi_http::bindings::http::types::ErrorCode;

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
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum SchemePattern {
    Http,
    Https,
    /// Matches both http and https (used with `*://` prefix).
    Any,
}

impl fmt::Display for SchemePattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SchemePattern::Http => write!(f, "http"),
            SchemePattern::Https => write!(f, "https"),
            SchemePattern::Any => write!(f, "*"),
        }
    }
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

/// Extract (scheme, host, port) from a URI.
fn extract_request_target(uri: &hyper::Uri) -> Option<(String, String, u16)> {
    let scheme = uri.scheme_str().unwrap_or("https").to_string();
    let host = uri.host()?.to_string();
    let default_port = if scheme == "https" { 443 } else { 80 };
    let port = uri.port_u16().unwrap_or(default_port);
    Some((scheme, host, port))
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum PolicyError {
    #[error("outgoing HTTP request has no host in URI: {0}")]
    RequestHasNoHost(Uri),
    #[error("outgoing HTTP {method} request to {scheme}://{host}:{port} denied")]
    RequestDenied {
        method: Method,
        scheme: String,
        host: String,
        port: u16,
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
        request: &mut hyper::Request<wasmtime_wasi_http::body::HyperOutgoingBody>,
    ) -> Result<(), PolicyError> {
        let Some((scheme, host, port)) = extract_request_target(request.uri()) else {
            return Err(PolicyError::RequestHasNoHost(request.uri().clone()));
        };
        let method = request.method().clone();

        // 1. Find matching host entries (check host + method)
        let matching: Vec<&AllowedHostPolicy> = self
            .hosts
            .iter()
            .filter(|h| h.pattern.matches(&scheme, &host, port, &method))
            .collect();
        if matching.is_empty() {
            return Err(PolicyError::RequestDenied {
                method,
                scheme,
                host,
                port,
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
        let Some((scheme, host, port)) = extract_request_target(uri) else {
            return Vec::new();
        };
        self.hosts
            .iter()
            .filter(|h| h.pattern.matches(&scheme, &host, port, method))
            .flat_map(|h| h.secrets.iter())
            .filter(|s| s.replace_in.contains(&ReplacementLocation::Body))
            .collect()
    }

    /// Perform async body replacement on a request.
    /// Must be called after `apply()` (which handles headers and params synchronously).
    /// Buffers the body, replaces placeholders in text content types, and re-wraps.
    pub(crate) async fn apply_body_replacement(
        &self,
        request: &mut hyper::Request<wasmtime_wasi_http::body::HyperOutgoingBody>,
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
