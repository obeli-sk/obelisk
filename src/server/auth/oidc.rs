//! OIDC ID tokens (JWTs) accepted as API bearer tokens.

use crate::config::server::OidcProviderConfig;
use anyhow::{Context as _, ensure};
use jsonwebtoken::{
    Algorithm, AlgorithmFamily, DecodingKey, DecodingKeyKind, Validation,
    crypto::{CryptoProvider, JwkUtils, JwtVerifier},
    errors::ErrorKind,
    jwk::JwkSet,
    signature,
};
use reqwest::Url;
use ring::signature as ring_sig;
use serde::Deserialize;
use serde_json::{Map, Value};
use std::{
    borrow::Cow,
    collections::BTreeMap,
    str::FromStr as _,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::{Mutex as AsyncMutex, MutexGuard};
use tracing::{debug, warn};

const JWKS_MAX_AGE: Duration = Duration::from_secs(3600);
pub(crate) const MIN_REFRESH_INTERVAL: Duration = if cfg!(test) {
    Duration::from_secs(1)
} else {
    Duration::from_secs(60)
};
const FETCH_TIMEOUT: Duration = Duration::from_secs(10);
const LEEWAY_SECS: u64 = 60;

pub(crate) struct OidcVerifier {
    issuers: Vec<Issuer>,
    http: reqwest::Client,
}

struct Issuer {
    iss: String,
    rules: Vec<Rule>,
    keys: Mutex<KeyCache>,
    refresh_lock: AsyncMutex<()>,
}

struct Rule {
    audience: String,
    claims: BTreeMap<String, String>,
}

#[derive(Default)]
struct KeyCache {
    jwks: Option<(JwkSet, Instant)>,
    last_attempt: Option<Instant>,
}

impl OidcVerifier {
    pub(crate) fn new(configs: &[OidcProviderConfig]) -> anyhow::Result<Option<Arc<Self>>> {
        if configs.is_empty() {
            return Ok(None);
        }
        install_crypto_provider()?;
        let mut issuers: Vec<Issuer> = Vec::new();
        for config in configs {
            check_url(&config.issuer)
                .with_context(|| format!("invalid `api.oidc` issuer {:?}", config.issuer))?;
            ensure!(
                !config.audience.is_empty(),
                "`api.oidc` audience of issuer {:?} must not be empty",
                config.issuer
            );
            ensure!(
                !config.claims.is_empty(),
                "`api.oidc` entry of issuer {:?} must constrain at least one claim, otherwise every \
                token of that issuer would be accepted",
                config.issuer
            );
            let rule = Rule {
                audience: config.audience.clone(),
                claims: config.claims.clone(),
            };
            match issuers.iter_mut().find(|i| i.iss == config.issuer) {
                Some(issuer) => issuer.rules.push(rule),
                None => issuers.push(Issuer {
                    iss: config.issuer.clone(),
                    rules: vec![rule],
                    keys: Mutex::default(),
                    refresh_lock: AsyncMutex::default(),
                }),
            }
        }
        let http = reqwest::Client::builder()
            .timeout(FETCH_TIMEOUT)
            .user_agent(concat!("obelisk/", env!("CARGO_PKG_VERSION")))
            .build()?;
        Ok(Some(Arc::new(Self { issuers, http })))
    }

    /// Fetch signing keys of all issuers in the background, surfacing misconfiguration early.
    pub(crate) fn spawn_prefetch(self: &Arc<Self>) {
        let this = Arc::clone(self);
        tokio::spawn(async move {
            for issuer in &this.issuers {
                let refresh = issuer.refresh_lock.lock().await;
                issuer.refresh(&this.http, refresh).await;
            }
        });
    }

    /// Verify a bearer token, returning the audit identity `oidc:<iss>#<sub>`.
    pub(crate) async fn verify(&self, token: &str) -> Result<String, Cow<'static, str>> {
        #[derive(Deserialize)]
        struct UnverifiedIssuer {
            iss: String,
        }

        let header = jsonwebtoken::decode_header(token).map_err(|_| "unknown token")?;
        let unverified = jsonwebtoken::dangerous::insecure_decode::<UnverifiedIssuer>(token)
            .map_err(|_| "malformed JWT")?;
        // The issuer is matched against configuration only; nothing is fetched for unknown issuers.
        let issuer = self
            .issuers
            .iter()
            .find(|issuer| issuer.iss == unverified.claims.iss)
            .ok_or("JWT from an untrusted issuer")?;
        let kid = header.kid.as_deref().ok_or("JWT without `kid`")?;
        let key = issuer.key(&self.http, kid, header.alg).await?;

        let mut validation = Validation::new(header.alg);
        validation.leeway = LEEWAY_SECS;
        validation.validate_nbf = true;
        validation.set_issuer(&[&issuer.iss]);
        validation.validate_aud = false; // Checked per rule below.
        validation.set_required_spec_claims(&["exp", "iss", "aud", "sub"]);
        let claims = jsonwebtoken::decode::<Map<String, Value>>(token, &key, &validation)
            .map_err(|err| describe_error(err.kind()))?
            .claims;
        let sub = claims
            .get("sub")
            .and_then(Value::as_str)
            .ok_or("JWT `sub` is not a string")?;

        let mut first_mismatch = None;
        for rule in &issuer.rules {
            match rule.check(&claims) {
                Ok(()) => return Ok(format!("oidc:{}#{sub}", issuer.iss)),
                Err(reason) => {
                    first_mismatch.get_or_insert(reason);
                }
            }
        }
        Err(first_mismatch.expect("every issuer has a rule").into())
    }
}

impl Issuer {
    async fn key(
        &self,
        http: &reqwest::Client,
        kid: &str,
        alg: Algorithm,
    ) -> Result<DecodingKey, &'static str> {
        let (has_key, stale) = {
            let cache = self.keys.lock().unwrap();
            let has_key = cache
                .jwks
                .as_ref()
                .is_some_and(|(jwks, _)| jwks.find(kid).is_some());
            let stale = cache
                .jwks
                .as_ref()
                .is_none_or(|(_jwks, fetched_at)| fetched_at.elapsed() >= JWKS_MAX_AGE || !has_key);
            (has_key, stale)
        };
        if stale {
            let refresh = if has_key {
                self.refresh_lock.try_lock().ok()
            } else {
                Some(self.refresh_lock.lock().await)
            };
            if let Some(refresh) = refresh {
                self.refresh(http, refresh).await;
            }
        }
        let cache = self.keys.lock().unwrap();
        let jwk = cache
            .jwks
            .as_ref()
            .and_then(|(jwks, _)| jwks.find(kid))
            .ok_or("JWT signed by an unknown key")?;
        if let Some(key_alg) = jwk.common.key_algorithm
            && Algorithm::from_str(&key_alg.to_string()).ok() != Some(alg)
        {
            return Err("JWT algorithm does not match its signing key");
        }
        DecodingKey::from_jwk(jwk).map_err(|_| "unusable signing key")
    }

    async fn refresh(&self, http: &reqwest::Client, _guard: MutexGuard<'_, ()>) {
        {
            let mut cache = self.keys.lock().unwrap();
            // Rate-limit unknown-key refreshes, including requests queued behind an in-flight fetch.
            if cache
                .last_attempt
                .is_some_and(|attempt| attempt.elapsed() < MIN_REFRESH_INTERVAL)
            {
                return;
            }
            cache.last_attempt = Some(Instant::now());
        }
        match fetch_jwks(http, &self.iss).await {
            Ok(jwks) => {
                debug!(
                    issuer = self.iss,
                    keys = jwks.keys.len(),
                    "Fetched OIDC signing keys"
                );
                self.keys.lock().unwrap().jwks = Some((jwks, Instant::now()));
            }
            Err(err) => warn!(
                issuer = self.iss,
                "Cannot fetch OIDC signing keys, keeping the previous ones: {err:#}"
            ),
        }
    }
}

impl Rule {
    fn check(&self, claims: &Map<String, Value>) -> Result<(), String> {
        let audience_matches = match claims.get("aud") {
            Some(Value::String(aud)) => *aud == self.audience,
            Some(Value::Array(auds)) => auds.iter().any(|aud| aud.as_str() == Some(&self.audience)),
            _ => false,
        };
        if !audience_matches {
            return Err("JWT audience mismatch".to_string());
        }
        match self
            .claims
            .iter()
            .find(|(name, expected)| claims.get(*name).and_then(Value::as_str) != Some(expected))
        {
            Some((name, _)) => Err(format!("JWT claim `{name}` mismatch")),
            None => Ok(()),
        }
    }
}

async fn fetch_jwks(http: &reqwest::Client, issuer: &str) -> anyhow::Result<JwkSet> {
    #[derive(Deserialize)]
    struct Discovery {
        issuer: String,
        jwks_uri: String,
    }

    let discovery_url = format!(
        "{}/.well-known/openid-configuration",
        issuer.trim_end_matches('/')
    );
    let discovery: Discovery = http
        .get(&discovery_url)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await
        .with_context(|| format!("cannot parse {discovery_url}"))?;
    ensure!(
        discovery.issuer == issuer,
        "discovery document names a different issuer {:?}",
        discovery.issuer
    );
    let jwks_uri = check_url(&discovery.jwks_uri).context("invalid `jwks_uri`")?;
    http.get(jwks_uri)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await
        .with_context(|| format!("cannot parse {}", discovery.jwks_uri))
}

fn check_url(url: &str) -> anyhow::Result<Url> {
    let parsed = Url::parse(url)?;
    let loopback = parsed.host_str().is_some_and(|host| {
        host == "localhost"
            || host
                .trim_start_matches('[')
                .trim_end_matches(']')
                .parse::<std::net::IpAddr>()
                .is_ok_and(|ip| ip.is_loopback())
    });
    ensure!(
        parsed.scheme() == "https" || (parsed.scheme() == "http" && loopback),
        "must use https unless the host is loopback"
    );
    Ok(parsed)
}

fn describe_error(kind: &ErrorKind) -> Cow<'static, str> {
    match kind {
        ErrorKind::ExpiredSignature => "expired JWT".into(),
        ErrorKind::ImmatureSignature => "JWT not yet valid".into(),
        ErrorKind::InvalidSignature => "invalid JWT signature".into(),
        ErrorKind::InvalidIssuer => "JWT issuer mismatch".into(),
        ErrorKind::InvalidAlgorithm => "unsupported JWT algorithm".into(),
        ErrorKind::MissingRequiredClaim(claim) => format!("JWT without `{claim}`").into(),
        other => format!("invalid JWT: {other:?}").into(),
    }
}

/// `jsonwebtoken` is built without a crypto backend; verification uses `ring`, already used by rustls.
static RING_PROVIDER: CryptoProvider = CryptoProvider {
    signer_factory: |_, _| Err(ErrorKind::InvalidAlgorithm.into()),
    verifier_factory: ring_verifier,
    jwk_utils: JwkUtils::new_unimplemented(),
};

fn install_crypto_provider() -> anyhow::Result<()> {
    match RING_PROVIDER.install_default() {
        Ok(()) => Ok(()),
        Err(existing) if std::ptr::eq(existing, &raw const RING_PROVIDER) => Ok(()),
        Err(_) => anyhow::bail!("another JWT crypto provider is already installed"),
    }
}

#[derive(Clone, Copy)]
enum RingAlgorithm {
    Rsa(&'static ring_sig::RsaParameters),
    Other(&'static dyn ring_sig::VerificationAlgorithm),
}

struct RingVerifier {
    alg: Algorithm,
    ring_alg: RingAlgorithm,
    key: DecodingKey,
}

/// Only asymmetric algorithms; `none` and HMAC are rejected.
fn ring_verifier(
    alg: &Algorithm,
    key: &DecodingKey,
) -> jsonwebtoken::errors::Result<Box<dyn JwtVerifier>> {
    let (family, ring_alg) = match alg {
        Algorithm::RS256 => (
            AlgorithmFamily::Rsa,
            RingAlgorithm::Rsa(&ring_sig::RSA_PKCS1_2048_8192_SHA256),
        ),
        Algorithm::RS384 => (
            AlgorithmFamily::Rsa,
            RingAlgorithm::Rsa(&ring_sig::RSA_PKCS1_2048_8192_SHA384),
        ),
        Algorithm::RS512 => (
            AlgorithmFamily::Rsa,
            RingAlgorithm::Rsa(&ring_sig::RSA_PKCS1_2048_8192_SHA512),
        ),
        Algorithm::PS256 => (
            AlgorithmFamily::Rsa,
            RingAlgorithm::Rsa(&ring_sig::RSA_PSS_2048_8192_SHA256),
        ),
        Algorithm::PS384 => (
            AlgorithmFamily::Rsa,
            RingAlgorithm::Rsa(&ring_sig::RSA_PSS_2048_8192_SHA384),
        ),
        Algorithm::PS512 => (
            AlgorithmFamily::Rsa,
            RingAlgorithm::Rsa(&ring_sig::RSA_PSS_2048_8192_SHA512),
        ),
        Algorithm::ES256 => (
            AlgorithmFamily::Ec,
            RingAlgorithm::Other(&ring_sig::ECDSA_P256_SHA256_FIXED),
        ),
        Algorithm::ES384 => (
            AlgorithmFamily::Ec,
            RingAlgorithm::Other(&ring_sig::ECDSA_P384_SHA384_FIXED),
        ),
        Algorithm::EdDSA => (
            AlgorithmFamily::Ed,
            RingAlgorithm::Other(&ring_sig::ED25519),
        ),
        Algorithm::HS256 | Algorithm::HS384 | Algorithm::HS512 => {
            return Err(ErrorKind::InvalidAlgorithm.into());
        }
    };
    if key.family() != family {
        return Err(ErrorKind::InvalidKeyFormat.into());
    }
    Ok(Box::new(RingVerifier {
        alg: *alg,
        ring_alg,
        key: key.clone(),
    }))
}

impl signature::Verifier<Vec<u8>> for RingVerifier {
    fn verify(&self, msg: &[u8], signature: &Vec<u8>) -> Result<(), signature::Error> {
        let result = match (self.key.kind(), self.ring_alg) {
            (DecodingKeyKind::RsaModulusExponent { n, e }, RingAlgorithm::Rsa(params)) => {
                ring_sig::RsaPublicKeyComponents { n, e }.verify(params, msg, signature)
            }
            (DecodingKeyKind::SecretOrDer(der), RingAlgorithm::Rsa(params)) => {
                ring_sig::UnparsedPublicKey::new(params, der).verify(msg, signature)
            }
            (DecodingKeyKind::SecretOrDer(bytes), RingAlgorithm::Other(alg)) => {
                ring_sig::UnparsedPublicKey::new(alg, bytes).verify(msg, signature)
            }
            (DecodingKeyKind::RsaModulusExponent { .. }, RingAlgorithm::Other(_)) => {
                Err(ring::error::Unspecified)
            }
        };
        result.map_err(|_| signature::Error::new())
    }
}

impl JwtVerifier for RingVerifier {
    fn algorithm(&self) -> Algorithm {
        self.alg
    }
}
