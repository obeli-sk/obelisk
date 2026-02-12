use hashbrown::HashSet;
use rand::RngCore;
use std::fmt;
use tracing::warn;
use wasmtime_wasi_http::bindings::http::types::ErrorCode;

/// Where in the outgoing request placeholders are replaced.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ReplacementLocation {
    Headers,
    Body,
    Params,
}

/// A secret that uses placeholder-based injection.
#[derive(Clone)]
pub struct PlaceholderSecret {
    /// The placeholder string exposed to WASM.
    pub placeholder: String,
    /// The real secret value.
    pub real_value: String,
    /// Hosts where replacement is allowed (scheme, host, port triples).
    pub allowed_hosts: HashSet<HostPattern>,
    /// Where in the request replacement is allowed.
    pub replace_in: HashSet<ReplacementLocation>,
}

impl fmt::Debug for PlaceholderSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PlaceholderSecret")
            .field("placeholder", &self.placeholder)
            .field("real_value", &"[REDACTED]")
            .field("allowed_hosts", &self.allowed_hosts)
            .field("replace_in", &self.replace_in)
            .finish()
    }
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

impl HostPattern {
    /// Parse a host specification string into a `HostPattern`.
    /// Rules:
    /// - No scheme → HTTPS assumed
    /// - No port → default for scheme (443 for HTTPS, 80 for HTTP)
    /// - `*` wildcard must be first or last character of host portion
    ///
    /// # Errors
    /// Returns an error if the wildcard is in the middle of the host.
    pub fn parse(input: &str) -> Result<Self, String> {
        let (scheme, rest) = if let Some(rest) = input.strip_prefix("https://") {
            ("https", rest)
        } else if let Some(rest) = input.strip_prefix("http://") {
            ("http", rest)
        } else {
            ("https", input)
        };

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
            return Err(format!(
                "wildcard `*` must be the first or last character in host pattern: `{host}`"
            ));
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
    /// Allowed host patterns. If `Some`, only matching hosts can be reached.
    /// `None` = no restrictions (when neither `allowed_hosts` nor secrets is set).
    pub allowed_hosts: Option<Vec<HostPattern>>,
    /// Placeholder secrets for substitution.
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

impl HttpRequestPolicy {
    /// Check if a host is allowed and perform secret placeholder replacement.
    /// Returns the (possibly modified) request, or an error if the host is denied.
    ///
    /// # Errors
    /// Returns `ErrorCode::HttpRequestDenied` if the request target is not allowed.
    pub fn apply(
        &self,
        request: &mut hyper::Request<wasmtime_wasi_http::body::HyperOutgoingBody>,
    ) -> Result<(), ErrorCode> {
        let Some((scheme, host, port)) = extract_request_target(request.uri()) else {
            warn!(
                "outgoing HTTP request has no host in URI: {}",
                request.uri()
            );
            return Err(ErrorCode::HttpRequestDenied);
        };

        // 1. Check allowlist
        if let Some(allowed) = &self.allowed_hosts {
            let is_allowed = allowed
                .iter()
                .any(|pattern| pattern.matches(&scheme, &host, port));
            if !is_allowed {
                warn!(
                    "outgoing HTTP request to {scheme}://{host}:{port} denied \
                     - host not in allowed_hosts"
                );
                return Err(ErrorCode::HttpRequestDenied);
            }
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
                        replaced = replaced.replace(&secret.placeholder, &secret.real_value);
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
                uri_replaced = uri_replaced.replace(&secret.placeholder, &secret.real_value);
            }
            if uri_replaced != uri_str
                && let Ok(new_uri) = uri_replaced.parse::<hyper::Uri>()
            {
                *request.uri_mut() = new_uri;
            }
        }

        // 5. Body replacement is handled asynchronously via `body_secrets_for`.

        Ok(())
    }

    /// Check if there are any body secrets applicable for the request's target host.
    #[must_use]
    pub fn has_body_secrets_for(&self, uri: &hyper::Uri) -> bool {
        let Some((scheme, host, port)) = extract_request_target(uri) else {
            return false;
        };
        self.secrets.iter().any(|s| {
            s.replace_in.contains(&ReplacementLocation::Body)
                && s.allowed_hosts
                    .iter()
                    .any(|pattern| pattern.matches(&scheme, &host, port))
        })
    }

    /// Get body secrets applicable for a given target.
    #[must_use]
    pub fn body_secrets_for(&self, uri: &hyper::Uri) -> Vec<&PlaceholderSecret> {
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
    pub env_mappings: Vec<(String, String)>,
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
    fn parse_host_pattern_wildcard_all() {
        let p = HostPattern::parse("*").unwrap();
        assert!(p.matches("https", "anything.com", 443));
    }

    #[test]
    fn parse_host_pattern_wildcard_middle_rejected() {
        assert!(HostPattern::parse("foo.*.com").is_err());
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
