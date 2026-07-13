//! Host-level bearer token check guarding the API port (web API, gRPC, gRPC-web).
//!
//! Applied as a middleware around the combined axum service before any routing,
//! so all three surfaces are authenticated uniformly; gRPC streams are checked
//! once at stream open. See `meta/designs/server-security-guard.md` (v1).

use crate::config::toml::ApiConfig;
use axum::{
    body::Body,
    extract::{Request, State},
    http::{HeaderMap, StatusCode, header},
    middleware::Next,
    response::Response,
};
use concepts::component_id::Digest;
use rand::Rng as _;
use secrecy::ExposeSecret as _;
use sha2::{Digest as _, Sha256};
use std::fmt::Write as _;
use std::sync::Arc;
use subtle::ConstantTimeEq as _;
use tracing::{debug, warn};

pub(crate) struct ApiAuth {
    /// Accepted token digests with the identity label used in audit logs.
    accepted: Vec<([u8; 32], String)>,
    /// Always-generated recovery token, printed to the console, valid until shutdown.
    startup_token: String,
    allow_all: bool,
}

impl ApiAuth {
    pub(crate) fn new(config: &ApiConfig, allow_all: bool) -> Self {
        let startup_token = generate_token();
        let mut accepted = vec![(sha256(&startup_token), "startup-token".to_string())];
        for digest in &config.token_hashes {
            accepted.push((digest.0, hash_prefix_label(digest)));
        }
        if let Some(token) = &config.token {
            let digest = sha256(token.expose_secret());
            accepted.push((digest, hash_prefix_label(&Digest(digest))));
        }
        Self {
            accepted,
            startup_token,
            allow_all,
        }
    }

    pub(crate) fn startup_token(&self) -> &str {
        &self.startup_token
    }

    fn check(&self, headers: &HeaderMap) -> Result<&str, &'static str> {
        if self.allow_all {
            return Ok("allow-all");
        }
        let presented = headers
            .get(header::AUTHORIZATION)
            .ok_or("missing `authorization` header")?
            .to_str()
            .map_err(|_| "malformed `authorization` header")?;
        let (scheme, token) = presented
            .split_once(' ')
            .ok_or("malformed `authorization` header")?;
        if !scheme.eq_ignore_ascii_case("bearer") {
            return Err("unsupported `authorization` scheme, expected `Bearer`");
        }
        // Hash-then-compare: digests of the secret are matched, so the comparison
        // cannot leak anything useful about accepted tokens.
        let digest = sha256(token.trim());
        self.accepted
            .iter()
            .find(|(accepted, _)| accepted.ct_eq(&digest).into())
            .map(|(_, identity)| identity.as_str())
            .ok_or("unknown token")
    }
}

pub(crate) async fn auth_middleware(
    State(auth): State<Arc<ApiAuth>>,
    req: Request,
    next: Next,
) -> Response {
    match auth.check(req.headers()) {
        Ok(identity) => {
            debug!(identity, path = %req.uri().path(), "Authorized API request");
            next.run(req).await
        }
        Err(reason) => {
            warn!(
                "Denied {} {}: {reason}. Clients must send `Authorization: Bearer <token>` \
                (CLI: `--api-token` or OBELISK_API_TOKEN). This server's startup token: {}",
                req.method(),
                req.uri().path(),
                auth.startup_token
            );
            deny_response(req.headers())
        }
    }
}

fn deny_response(headers: &HeaderMap) -> Response {
    let grpc_content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|content_type| content_type.to_str().ok())
        .filter(|content_type| content_type.starts_with("application/grpc"));
    if let Some(content_type) = grpc_content_type {
        // Trailers-only gRPC response; 16 = UNAUTHENTICATED.
        Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, content_type)
            .header("grpc-status", "16")
            .header("grpc-message", "missing or invalid API token")
            .body(Body::empty())
            .expect("static response must build")
    } else {
        Response::builder()
            .status(StatusCode::UNAUTHORIZED)
            .header(header::WWW_AUTHENTICATE, "Bearer")
            .body(Body::from("missing or invalid API token"))
            .expect("static response must build")
    }
}

fn sha256(token: &str) -> [u8; 32] {
    Sha256::digest(token.as_bytes()).into()
}

/// Digest of a token as accepted in `api.token_hashes`; displays as `sha256:<hex>`.
pub(crate) fn token_hash(token: &str) -> Digest {
    Digest(sha256(token))
}

/// Generate a high-entropy random token (64 hex chars).
pub(crate) fn generate_token() -> String {
    hex(&rand::rng().random::<[u8; 32]>())
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        write!(&mut out, "{b:02x}").expect("writing to string");
    }
    out
}

/// Audit-log identity of a `token_hashes` entry: the first 8 hex chars of its digest.
fn hash_prefix_label(digest: &Digest) -> String {
    format!("token:{}", &hex(&digest.0)[..8])
}
