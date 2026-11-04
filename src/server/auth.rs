//! Host-level bearer token check guarding the API port (web API, gRPC, gRPC-web).
//!
//! Applied as a middleware around the combined axum service before any routing,
//! so all three surfaces are authenticated uniformly; gRPC streams are checked
//! once at stream open. See `meta/designs/server-security-guard.md` (v1).

pub(crate) mod oidc;

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
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use subtle::ConstantTimeEq as _;
use tracing::{trace, warn};

pub(crate) struct ApiAuth {
    accepted: Vec<AcceptedToken>,
    /// Internal token, printed only with `--dev`, valid until shutdown.
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
        accepted.extend(
            config
                .token_hashes
                .iter()
                .map(|digest| AcceptedToken::new(digest.0, hash_prefix_label(digest))),
        );
        let oidc = oidc::OidcVerifier::new(&config.oidc)?;
        if let Some(oidc) = &oidc {
            oidc.spawn_prefetch();
        }
        Ok(Self {
            accepted,
            startup_token,
            oidc,
        })
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
        let token = token.trim();
        if let Some(identity) = self.check_static(token) {
            Ok(identity)
        } else {
            match &self.oidc {
                Some(oidc) => oidc.verify(token).await,
                None => Err("unknown token".into()),
            }
        }
    }

    /// Match the startup token, `--api-token` and `token_hashes`, returning the audit identity.
    fn check_static(&self, token: &str) -> Option<String> {
        // Hash-then-compare: digests of the secret are matched, so the comparison
        // cannot leak anything useful about accepted tokens.
        let digest = crate::api::token_digest(token);
        self.accepted
            .iter()
            .find_map(|accepted| accepted.check(&digest, token.len()))
    }
}

/// Bearer tokens accepted by an `http_server`: its own `oidc` entries, and with `api_auth`
/// every token the API accepts. Any single match grants access.
pub(crate) struct HttpServerAuth {
    oidc: Option<Arc<oidc::OidcVerifier>>,
    api: Option<Arc<ApiAuth>>,
}

impl std::fmt::Debug for HttpServerAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpServerAuth")
            .field("oidc", &self.oidc.is_some())
            .field(
                "api",
                &match &self.api {
                    None => "off",
                    Some(_) => "checked",
                },
            )
            .finish()
    }
}

impl HttpServerAuth {
    pub(crate) fn new(
        server: &crate::config::server::HttpServer,
        api: Option<&Arc<ApiAuth>>,
    ) -> Result<Option<Arc<Self>>, anyhow::Error> {
        let oidc = oidc::OidcVerifier::new(&server.oidc)?;
        if let Some(oidc) = &oidc {
            oidc.spawn_prefetch();
        }
        let api = if server.api_auth {
            Some(
                api.cloned()
                    .ok_or_else(|| anyhow::anyhow!("`api_auth` requires an enabled API"))?,
            )
        } else {
            None
        };
        Ok((oidc.is_some() || api.is_some()).then(|| Arc::new(Self { oidc, api })))
    }
}

#[async_trait::async_trait]
impl wasm_workers::webhook::webhook_trigger::WebhookRequestAuth for HttpServerAuth {
    async fn authenticate(
        &self,
        headers: &HeaderMap,
    ) -> Result<Option<header::HeaderValue>, StatusCode> {
        let token = bearer_token(headers);
        let token = token.ok_or(StatusCode::UNAUTHORIZED)?;
        let mut status = StatusCode::UNAUTHORIZED;
        if let Some(api) = &self.api
            && api.check_static(token).is_some()
        {
            return Ok(None);
        }
        let api_oidc = self.api.as_ref().and_then(|api| api.oidc.as_ref());
        for oidc in self.oidc.iter().chain(api_oidc) {
            match oidc.claims_header(token).await {
                Ok(claims) => return Ok(Some(claims)),
                Err(StatusCode::FORBIDDEN) => status = StatusCode::FORBIDDEN,
                Err(_) => {}
            }
        }
        Err(status)
    }
}

/// The token of a single `Authorization: Bearer` header.
fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    let mut authorization = headers.get_all(header::AUTHORIZATION).iter();
    let presented = authorization.next()?.to_str().ok()?;
    if authorization.next().is_some() {
        return None;
    }
    let (scheme, token) = presented.split_once(' ')?;
    let token = token.trim();
    (scheme.eq_ignore_ascii_case("bearer") && !token.is_empty()).then_some(token)
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
            deny_response()
        }
    }
}

fn deny_response() -> Response {
    Response::builder()
        .status(StatusCode::UNAUTHORIZED)
        .header(header::WWW_AUTHENTICATE, "Bearer")
        .body(Body::from("missing or invalid API token"))
        .expect("static response must build")
}

/// Audit-log identity of a `token_hashes` entry: the first 8 hex chars of its digest.
fn hash_prefix_label(digest: &concepts::component_id::Digest) -> String {
    format!("token:{}", &digest.to_string()["sha256:".len()..][..8])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::server::RuntimeConfigAvailability;
    use crate::config::{
        app::AppConfigToml,
        env_var::StartupEnvVars,
        secret_registry::{EnvVarSecretsCleanup, PublicEnvToml, SecretRegistry},
    };

    #[tokio::test]
    async fn internal_secret_authenticates_without_an_operator_token() {
        let app: AppConfigToml = toml::from_str(
            "app_name = 'test-app'\n[secrets]\nINTERNAL_TOKEN = { type = 'internal' }",
        )
        .unwrap();
        let registry = SecretRegistry::resolve(
            app.secrets,
            PublicEnvToml::default(),
            EnvVarSecretsCleanup::Noop,
            RuntimeConfigAvailability::Strict,
            &StartupEnvVars::capture(),
        )
        .unwrap();
        let auth = ApiAuth::new(&ApiConfig::default(), None).unwrap();
        let registry = registry
            .with_internal_token(Some(auth.startup_token()))
            .unwrap();
        let token = registry.secret_lookup("INTERNAL_TOKEN").unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            format!("Bearer {}", token.expose_secret()).parse().unwrap(),
        );
        assert_eq!(auth.check(&headers).await.unwrap(), "startup-token");
        let restarted = ApiAuth::new(&ApiConfig::default(), None).unwrap();
        assert!(restarted.check(&headers).await.is_err());
    }
}
