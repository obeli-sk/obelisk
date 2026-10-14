//! Host-level bearer token check guarding the API port (web API, gRPC, gRPC-web).
//!
//! Applied as a middleware around the combined axum service before any routing,
//! so all three surfaces are authenticated uniformly; gRPC streams are checked
//! once at stream open. See `meta/designs/server-security-guard.md` (v1).

pub(crate) mod oidc;
pub(crate) mod reload;

use crate::config::server::ApiConfig;
use axum::{
    body::Body,
    extract::{Request, State},
    http::{HeaderMap, StatusCode, header},
    middleware::Next,
    response::Response,
};
use secrecy::{ExposeSecret as _, SecretString};
use std::{
    borrow::Cow,
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, Ordering},
    },
};
use subtle::ConstantTimeEq as _;
use tracing::{trace, warn};

pub(crate) struct ApiAuth {
    accepted: Vec<AcceptedToken>,
    configured: RwLock<Vec<AcceptedToken>>,
    /// Recovery token, printed to the console, valid until shutdown.
    startup_token: SecretString,
    oidc: Option<Arc<oidc::OidcVerifier>>,
}

struct AcceptedToken {
    digest: [u8; 32],
    identity: String,
    warned_short: AtomicBool,
}

impl AcceptedToken {
    fn new(digest: [u8; 32], identity: String) -> Self {
        Self {
            digest,
            identity,
            warned_short: AtomicBool::default(),
        }
    }

    fn check(&self, digest: &[u8; 32], token_length: usize) -> Option<String> {
        if !bool::from(self.digest.ct_eq(digest)) {
            return None;
        }
        // Dev convenience: Accept tokens shorter than 32 characters, but emit a warning.
        if token_length < crate::api::MIN_API_TOKEN_CHAR_LENGTH
            && !self.warned_short.swap(true, Ordering::Relaxed)
        {
            warn!(
                identity = self.identity,
                length = token_length,
                minimum = crate::api::MIN_API_TOKEN_CHAR_LENGTH,
                "Accepted a short API token; replace it with `obelisk generate token`"
            );
        }
        Some(self.identity.clone())
    }
}

impl ApiAuth {
    pub(crate) fn new(
        config: &ApiConfig,
        api_token: Option<SecretString>,
    ) -> Result<Self, anyhow::Error> {
        let startup_token = SecretString::from(crate::api::generate_token());
        let mut accepted = vec![AcceptedToken::new(
            crate::api::token_digest(startup_token.expose_secret()),
            "startup-token".to_string(),
        )];
        if let Some(token) = api_token {
            let digest = crate::api::token_hash(token.expose_secret());
            accepted.push(AcceptedToken::new(digest.0, hash_prefix_label(&digest)));
        }
        let oidc = oidc::OidcVerifier::new(&config.oidc)?;
        if let Some(oidc) = &oidc {
            oidc.spawn_prefetch();
        }
        Ok(Self {
            accepted,
            configured: RwLock::new(Self::configured_tokens(&config.token_hashes)),
            startup_token,
            oidc,
        })
    }

    fn configured_tokens(hashes: &[concepts::component_id::Digest]) -> Vec<AcceptedToken> {
        hashes
            .iter()
            .map(|digest| AcceptedToken::new(digest.0, hash_prefix_label(digest)))
            .collect()
    }

    fn replace_configured(&self, hashes: &[concepts::component_id::Digest]) {
        let configured = Self::configured_tokens(hashes);
        *self.configured.write().unwrap() = configured;
    }

    pub(crate) fn startup_token(&self) -> &SecretString {
        &self.startup_token
    }

    async fn check(&self, headers: &HeaderMap) -> Result<String, Cow<'static, str>> {
        let presented = headers
            .get(header::AUTHORIZATION)
            .ok_or("missing `authorization` header")?
            .to_str()
            .map_err(|_| "malformed `authorization` header")?;
        let (scheme, token) = presented
            .split_once(' ')
            .ok_or("malformed `authorization` header")?;
        if !scheme.eq_ignore_ascii_case("bearer") {
            return Err("unsupported `authorization` scheme, expected `Bearer`".into());
        }
        // Hash-then-compare: digests of the secret are matched, so the comparison
        // cannot leak anything useful about accepted tokens.
        let token = token.trim();
        let digest = crate::api::token_digest(token);
        let identity = self
            .accepted
            .iter()
            .find_map(|accepted| accepted.check(&digest, token.len()))
            .or_else(|| {
                self.configured
                    .read()
                    .unwrap()
                    .iter()
                    .find_map(|accepted| accepted.check(&digest, token.len()))
            });
        if let Some(identity) = identity {
            Ok(identity)
        } else {
            match &self.oidc {
                Some(oidc) => oidc.verify(token).await,
                None => Err("unknown token".into()),
            }
        }
    }
}

pub(crate) async fn auth_middleware(
    State(auth): State<Arc<ApiAuth>>,
    req: Request,
    next: Next,
) -> Response {
    match auth.check(req.headers()).await {
        Ok(identity) => {
            trace!(%identity, path = %req.uri().path(), "Authorized API request");
            next.run(req).await
        }
        Err(reason) => {
            warn!(
                "Denied {} {}: {reason}. Clients must send `Authorization: Bearer <token>` \
                (CLI: `--api-token` or OBELISK_API_TOKEN).",
                req.method(),
                req.uri().path(),
            );
            // Not `tracing`: log sinks (files, OTLP) are shipped and readable by anyone with log access, while the token grants full admin.
            eprintln!(
                "This server's startup token: {}",
                auth.startup_token.expose_secret()
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

/// Audit-log identity of a `token_hashes` entry: the first 8 hex chars of its digest.
fn hash_prefix_label(digest: &concepts::component_id::Digest) -> String {
    format!("token:{}", &digest.to_string()["sha256:".len()..][..8])
}
