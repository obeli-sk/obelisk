use hashbrown::HashSet;
use hyper::Uri;
use rand::RngCore;
use secrecy::{ExposeSecret, SecretString};
use std::{fmt, net::SocketAddr, sync::Arc};
use tracing::debug;
use wasmtime_wasi_http::bindings::http::types::ErrorCode;

/// Where in the outgoing request placeholders are replaced.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ReplacementLocation {
    Headers,
    Body,
    Params,
}

/// A secret that uses placeholder-based injection.
#[derive(Clone, Debug)]
pub struct PlaceholderSecret {
    /// The placeholder string exposed to WASM.
    pub placeholder: String,
    /// The real secret value.
    pub real_value: SecretString,
    /// Hosts where replacement is allowed (scheme, host, port triples).
    pub allowed_hosts: HashSet<HostPattern>,
    /// Where in the request replacement is allowed.
    pub replace_in: HashSet<ReplacementLocation>,
}

/// A parsed host pattern for matching outgoing requests.
/// Supports wildcards: `*` means all hosts, `*.example.com` matches subdomains,
/// `192.168.1.*` matches a /24 range.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct HostPattern {
    pub scheme: String,
    pub host_pattern: String,
    pub port: u16,
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
    /// - No port → default for scheme (443 for HTTPS, 80 for HTTP)
    /// - `*` wildcard must be first or last character of host portion
    ///
    /// # Errors
    /// Returns an error if the wildcard is in the middle of the host.
    pub fn parse(input: &str) -> Result<Self, HostPatternError> {
        let (scheme, rest) = if let Some(rest) = input.strip_prefix("https://") {
            ("https", rest)
        } else if let Some(rest) = input.strip_prefix("http://") {
            ("http", rest)
        } else {
            ("https", input)
        };

        // Reject patterns that contain a path (e.g. "http://localhost:1234/")
        if rest.contains('/') {
            return Err(HostPatternError::ContainsPath {
                input: input.to_string(),
            });
        }

        let (host, port) = if let Some((h, p)) = rest.rsplit_once(':') {
            if let Ok(port) = p.parse::<u16>() {
                (h.to_string(), port)
            } else {
                // Not a valid port, treat the whole thing as host
                let default_port = if scheme == "https" { 443 } else { 80 };
                (rest.to_string(), default_port)
            }
        } else {
            let default_port = if scheme == "https" { 443 } else { 80 };
            (rest.to_string(), default_port)
        };

        // Validate wildcard: must be first or last character
        if host.contains('*') && !host.starts_with('*') && !host.ends_with('*') {
            return Err(HostPatternError::Wildcard { host });
        }

        Ok(HostPattern {
            scheme: scheme.to_string(),
            host_pattern: host,
            port,
        })
    }

    /// Check if a (scheme, host, port) triple matches this pattern.
    #[must_use]
    pub fn matches(&self, scheme: &str, host: &str, port: u16) -> bool {
        if self.scheme != scheme || self.port != port {
            return false;
        }
        match_wildcard(&self.host_pattern, host)
    }
}

impl HostPattern {
    /// Check if this pattern could match a given socket address.
    /// Used to warn when `allowed_hosts` could reach the server's own ports.
    /// This is best-effort: it checks localhost variants, 127.0.0.0/8, 0.0.0.0,
    /// `[::1]`, `[::]`, and wildcard patterns.
    #[must_use]
    pub fn could_match_socket_addr(&self, addr: &SocketAddr) -> bool {
        // Only HTTP scheme can reach local services
        if self.scheme != "http" {
            return false;
        }
        if self.port != addr.port() {
            return false;
        }
        let addr_ip = addr.ip();
        let is_local_addr = addr_ip.is_loopback()
            || addr_ip.is_unspecified() // 0.0.0.0 or [::]
            || addr == &SocketAddr::from(([127, 0, 0, 1], addr.port()));

        if !is_local_addr {
            // The server is bound to a specific non-local IP.
            // Check if the pattern matches that specific IP.
            let ip_str = addr_ip.to_string();
            return match_wildcard(&self.host_pattern, &ip_str);
        }

        // Server is bound to a local address. Check if the host pattern
        // could resolve to localhost.
        let host = &self.host_pattern;

        // Exact wildcard matches everything
        if host == "*" {
            return true;
        }

        // Check common localhost names
        let localhost_names = ["localhost", "127.0.0.1", "[::1]", "0.0.0.0", "[::]"];
        for name in &localhost_names {
            if match_wildcard(host, name) {
                return true;
            }
        }

        // Check 127.x.y.z patterns (the whole 127.0.0.0/8 block is loopback)
        if host.starts_with("127.") || host == "127.*" {
            return true;
        }
        // Suffix wildcard like "12*" would match "127.0.0.1"
        if host.ends_with('*') {
            let prefix = &host[..host.len() - 1];
            if "127.0.0.1".starts_with(prefix)
                || "localhost".starts_with(prefix)
                || "[::1]".starts_with(prefix)
            {
                return true;
            }
        }
        // Prefix wildcard like "*1" would match "127.0.0.1" or "[::1]"
        if let Some(suffix) = host.strip_prefix('*')
            && ("127.0.0.1".ends_with(suffix)
                || "localhost".ends_with(suffix)
                || "[::1]".ends_with(suffix))
        {
            return true;
        }

        false
    }
}

impl fmt::Display for HostPattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let default_port = if self.scheme == "https" { 443 } else { 80 };
        if self.port == default_port {
            write!(f, "{}://{}", self.scheme, self.host_pattern)
        } else {
            write!(f, "{}://{}:{}", self.scheme, self.host_pattern, self.port)
        }
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

/// Per-component HTTP outgoing request policy.
#[derive(Clone, Debug, Default)]
pub struct HttpRequestPolicy {
    pub allowed_hosts: Arc<[HostPattern]>,
    pub secrets: Vec<PlaceholderSecret>,
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
    #[error("outgoing HTTP request to {scheme}://{host}:{port} denied")]
    RequestDenied {
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

        // 1. Check allowlist
        let is_allowed = self
            .allowed_hosts
            .iter()
            .any(|pattern| pattern.matches(&scheme, &host, port));
        if !is_allowed {
            return Err(PolicyError::RequestDenied { scheme, host, port });
        }

        // 2. Collect applicable secrets for this host
        let applicable: Vec<&PlaceholderSecret> = self
            .secrets
            .iter()
            .filter(|s| {
                s.allowed_hosts
                    .iter()
                    .any(|pattern| pattern.matches(&scheme, &host, port))
            })
            .collect();

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

    /// Get body secrets applicable for the request's target host.
    fn body_secrets_for(&self, uri: &hyper::Uri) -> Vec<&PlaceholderSecret> {
        let Some((scheme, host, port)) = extract_request_target(uri) else {
            return Vec::new();
        };
        self.secrets
            .iter()
            .filter(|s| {
                s.replace_in.contains(&ReplacementLocation::Body)
                    && s.allowed_hosts
                        .iter()
                        .any(|pattern| pattern.matches(&scheme, &host, port))
            })
            .collect()
    }

    /// Perform async body replacement on a request.
    /// Must be called after `apply()` (which handles headers and params synchronously).
    /// Buffers the body, replaces placeholders in text content types, and re-wraps.
    pub(crate) async fn apply_body_replacement(
        &self,
        request: &mut hyper::Request<wasmtime_wasi_http::body::HyperOutgoingBody>,
    ) {
        let body_secrets = self.body_secrets_for(request.uri());
        if body_secrets.is_empty() {
            debug!("No secrets, no modifications to HTTP body");
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

/// Resolved secret configuration from TOML (real values resolved, no placeholders yet).
#[derive(Clone, Debug)]
pub struct SecretConfig {
    pub hosts: HashSet<HostPattern>,
    /// `(env_key_for_wasm, real_value)` pairs.
    pub env_mappings: Vec<(String, SecretString)>,
    /// Where in the request to perform replacement.
    pub replace_in: HashSet<ReplacementLocation>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_host_pattern_bare_hostname() {
        let p = HostPattern::parse("api.openai.com").unwrap();
        assert_eq!(p.scheme, "https");
        assert_eq!(p.host_pattern, "api.openai.com");
        assert_eq!(p.port, 443);
        assert!(p.matches("https", "api.openai.com", 443));
        assert!(!p.matches("http", "api.openai.com", 80));
    }

    #[test]
    fn parse_host_pattern_with_scheme_and_port() {
        let p = HostPattern::parse("http://localhost:8080").unwrap();
        assert_eq!(p.scheme, "http");
        assert_eq!(p.host_pattern, "localhost");
        assert_eq!(p.port, 8080);
        assert!(p.matches("http", "localhost", 8080));
        assert!(!p.matches("https", "localhost", 8080));
    }

    #[test]
    fn parse_host_pattern_http_default_port() {
        let p = HostPattern::parse("http://example.com").unwrap();
        assert_eq!(p.scheme, "http");
        assert_eq!(p.host_pattern, "example.com");
        assert_eq!(p.port, 80);
    }

    #[test]
    fn parse_host_pattern_wildcard_prefix() {
        let p = HostPattern::parse("*.example.com").unwrap();
        assert!(p.matches("https", "api.example.com", 443));
        assert!(p.matches("https", "foo.bar.example.com", 443));
        assert!(!p.matches("https", "example.com", 443));
    }

    #[test]
    fn parse_host_pattern_wildcard_suffix() {
        let p = HostPattern::parse("192.168.1.*").unwrap();
        assert!(p.matches("https", "192.168.1.100", 443));
        assert!(!p.matches("https", "192.168.2.100", 443));
    }

    #[test]
    fn parse_host_pattern_wildcard_all_https() {
        let p = HostPattern::parse("*").unwrap();
        assert!(p.matches("https", "anything.com", 443));
        assert!(!p.matches("http", "anything.com", 80));
    }

    #[test]
    fn parse_host_pattern_wildcard_http() {
        let p = HostPattern::parse("http://*").unwrap();
        assert!(!p.matches("https", "anything.com", 443));
        assert!(p.matches("http", "anything.com", 80));
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
        assert_eq!(p.scheme, "https");
        assert_eq!(p.host_pattern, "internal.corp.com");
        assert_eq!(p.port, 8443);
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
    fn could_match_socket_addr_localhost_5005() {
        let addr: SocketAddr = "127.0.0.1:5005".parse().unwrap();
        // Exact match
        assert!(
            HostPattern::parse("http://127.0.0.1:5005")
                .unwrap()
                .could_match_socket_addr(&addr)
        );
        // localhost alias
        assert!(
            HostPattern::parse("http://localhost:5005")
                .unwrap()
                .could_match_socket_addr(&addr)
        );
        // Wildcard host
        assert!(
            HostPattern::parse("http://*:5005")
                .unwrap()
                .could_match_socket_addr(&addr)
        );
        // 127.* prefix wildcard
        assert!(
            HostPattern::parse("http://127.*:5005")
                .unwrap()
                .could_match_socket_addr(&addr)
        );
        // Wrong port
        assert!(
            !HostPattern::parse("http://127.0.0.1:9999")
                .unwrap()
                .could_match_socket_addr(&addr)
        );
        // HTTPS scheme doesn't match local HTTP
        assert!(
            !HostPattern::parse("https://127.0.0.1:5005")
                .unwrap()
                .could_match_socket_addr(&addr)
        );
        // Different host entirely
        assert!(
            !HostPattern::parse("http://api.example.com:5005")
                .unwrap()
                .could_match_socket_addr(&addr)
        );
    }

    #[test]
    fn could_match_socket_addr_unspecified() {
        // 0.0.0.0 accepts traffic on all interfaces
        let addr: SocketAddr = "0.0.0.0:8080".parse().unwrap();
        assert!(
            HostPattern::parse("http://localhost:8080")
                .unwrap()
                .could_match_socket_addr(&addr)
        );
        assert!(
            HostPattern::parse("http://127.0.0.1:8080")
                .unwrap()
                .could_match_socket_addr(&addr)
        );
        assert!(
            HostPattern::parse("http://0.0.0.0:8080")
                .unwrap()
                .could_match_socket_addr(&addr)
        );
        assert!(
            HostPattern::parse("http://*:8080")
                .unwrap()
                .could_match_socket_addr(&addr)
        );
    }

    #[test]
    fn could_match_socket_addr_ipv6_loopback() {
        let addr: SocketAddr = "[::1]:5005".parse().unwrap();
        assert!(
            HostPattern::parse("http://[::1]:5005")
                .unwrap()
                .could_match_socket_addr(&addr)
        );
        assert!(
            HostPattern::parse("http://localhost:5005")
                .unwrap()
                .could_match_socket_addr(&addr)
        );
        assert!(
            HostPattern::parse("http://*:5005")
                .unwrap()
                .could_match_socket_addr(&addr)
        );
    }

    #[test]
    fn could_match_socket_addr_ipv6_unspecified() {
        let addr: SocketAddr = "[::]:5005".parse().unwrap();
        assert!(
            HostPattern::parse("http://localhost:5005")
                .unwrap()
                .could_match_socket_addr(&addr)
        );
        assert!(
            HostPattern::parse("http://[::]:5005")
                .unwrap()
                .could_match_socket_addr(&addr)
        );
        assert!(
            HostPattern::parse("http://*:5005")
                .unwrap()
                .could_match_socket_addr(&addr)
        );
    }

    #[test]
    fn could_match_socket_addr_suffix_wildcard() {
        let addr: SocketAddr = "127.0.0.1:5005".parse().unwrap();
        // "*1" matches "127.0.0.1" (ends with 1) and "[::1]"
        assert!(
            HostPattern::parse("http://*1:5005")
                .unwrap()
                .could_match_socket_addr(&addr)
        );
        // "*host" matches "localhost"
        assert!(
            HostPattern::parse("http://*host:5005")
                .unwrap()
                .could_match_socket_addr(&addr)
        );
    }

    #[test]
    fn could_match_socket_addr_non_local_binding() {
        // Server bound to a specific public IP
        let addr: SocketAddr = "192.168.1.100:5005".parse().unwrap();
        assert!(
            HostPattern::parse("http://192.168.1.100:5005")
                .unwrap()
                .could_match_socket_addr(&addr)
        );
        assert!(
            HostPattern::parse("http://192.168.1.*:5005")
                .unwrap()
                .could_match_socket_addr(&addr)
        );
        assert!(
            !HostPattern::parse("http://localhost:5005")
                .unwrap()
                .could_match_socket_addr(&addr)
        );
        assert!(
            !HostPattern::parse("http://127.0.0.1:5005")
                .unwrap()
                .could_match_socket_addr(&addr)
        );
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
