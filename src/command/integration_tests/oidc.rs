use super::*;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use ring::{
    rand::SystemRandom,
    signature::{self as ring_sig, KeyPair as _},
};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use tokio::sync::Notify;

const AUDIENCE: &str = "https://obelisk.test";

#[derive(Default)]
struct JwksPause {
    next: AtomicBool,
    started: Notify,
    resume: Notify,
}

enum Signer {
    Rsa(ring_sig::RsaKeyPair),
    Ed25519(ring_sig::Ed25519KeyPair),
}

impl Signer {
    fn jwk(&self, kid: &str) -> Value {
        match self {
            Signer::Rsa(key) => {
                let components = ring_sig::RsaPublicKeyComponents::<Vec<u8>>::from(key.public());
                json!({
                    "kid": kid, "kty": "RSA", "alg": "RS256", "use": "sig",
                    "n": URL_SAFE_NO_PAD.encode(components.n),
                    "e": URL_SAFE_NO_PAD.encode(components.e),
                })
            }
            Signer::Ed25519(key) => json!({
                "kid": kid, "kty": "OKP", "crv": "Ed25519", "alg": "EdDSA", "use": "sig",
                "x": URL_SAFE_NO_PAD.encode(key.public_key().as_ref()),
            }),
        }
    }

    fn token(&self, kid: &str, claims: &Value) -> String {
        let alg = match self {
            Signer::Rsa(_) => "RS256",
            Signer::Ed25519(_) => "EdDSA",
        };
        let message = format!(
            "{}.{}",
            URL_SAFE_NO_PAD.encode(json!({"alg": alg, "kid": kid, "typ": "JWT"}).to_string()),
            URL_SAFE_NO_PAD.encode(claims.to_string())
        );
        let signature = match self {
            Signer::Rsa(key) => {
                let mut signature = vec![0; key.public().modulus_len()];
                key.sign(
                    &ring_sig::RSA_PKCS1_SHA256,
                    &SystemRandom::new(),
                    message.as_bytes(),
                    &mut signature,
                )
                .unwrap();
                signature
            }
            Signer::Ed25519(key) => key.sign(message.as_bytes()).as_ref().to_vec(),
        };
        format!("{message}.{}", URL_SAFE_NO_PAD.encode(signature))
    }
}

/// Serves OIDC discovery and a mutable JWKS on a loopback port.
async fn spawn_issuer(ip: &str, jwks: Arc<Mutex<Value>>, pause: Arc<JwksPause>) -> String {
    let listener = tokio::net::TcpListener::bind(format!("{ip}:0"))
        .await
        .unwrap();
    let issuer = format!("http://{}", listener.local_addr().unwrap());
    let discovery = json!({"issuer": issuer, "jwks_uri": format!("{issuer}/.well-known/jwks")});
    let router = axum::Router::new()
        .route(
            "/.well-known/openid-configuration",
            axum::routing::get(move || async move { axum::Json(discovery) }),
        )
        .route(
            "/.well-known/jwks",
            axum::routing::get(move || {
                let jwks = Arc::clone(&jwks);
                let pause = Arc::clone(&pause);
                async move {
                    if pause.next.swap(false, Ordering::Relaxed) {
                        pause.started.notify_one();
                        pause.resume.notified().await;
                    }
                    axum::Json(jwks.lock().unwrap().clone())
                }
            }),
        );
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    issuer
}

#[tokio::test]
async fn api_auth_should_accept_oidc_tokens() {
    let ip = test_addr!(191);
    let rsa = Signer::Rsa(
        ring_sig::RsaKeyPair::from_der(include_bytes!("oidc_rsa_test_key.der")).unwrap(),
    );
    let jwks = Arc::new(Mutex::new(json!({"keys": [rsa.jwk("rsa-1")]})));
    let pause = Arc::new(JwksPause::default());
    let issuer = spawn_issuer(&ip, jwks.clone(), Arc::clone(&pause)).await;
    let server = TestServer::start_empty_with_auth(
        ip,
        &format!(
            r#"api.oidc = [{{ issuer = "{issuer}", audience = "{AUDIENCE}", claims = {{ app_name = "deployer" }} }}]"#
        ),
        None,
    )
    .await;

    let now = jsonwebtoken::get_current_timestamp();
    let claims = |overrides: Value| {
        let mut claims = json!({
            "iss": issuer, "aud": AUDIENCE, "sub": "org:deployer:machine",
            "app_name": "deployer", "iat": now, "nbf": now, "exp": now + 600,
        });
        claims
            .as_object_mut()
            .unwrap()
            .extend(overrides.as_object().unwrap().clone());
        claims
    };
    // Keeps a valid payload and signature, swapping the header's `alg`.
    let with_alg = |alg: &str| {
        let valid = rsa.token("rsa-1", &claims(json!({})));
        let (_, payload_and_signature) = valid.split_once('.').unwrap();
        let header = URL_SAFE_NO_PAD.encode(json!({"alg": alg, "kid": "rsa-1"}).to_string());
        format!("{header}.{payload_and_signature}")
    };
    let url = format!("{}/v1/functions", server.base_url);
    let status = async |token: String| {
        server
            .client
            .get(&url)
            .bearer_auth(token)
            .send()
            .await
            .unwrap()
            .status()
    };

    assert!(
        status(rsa.token("rsa-1", &claims(json!({}))))
            .await
            .is_success()
    );
    for (rejected, token) in [
        (
            "wrong audience",
            rsa.token("rsa-1", &claims(json!({"aud": "sts.amazonaws.com"}))),
        ),
        (
            "expired",
            rsa.token("rsa-1", &claims(json!({"exp": now - 3600}))),
        ),
        (
            "not yet valid",
            rsa.token("rsa-1", &claims(json!({"nbf": now + 300}))),
        ),
        (
            "claim mismatch",
            rsa.token("rsa-1", &claims(json!({"app_name": "other"}))),
        ),
        (
            "untrusted issuer",
            rsa.token(
                "rsa-1",
                &claims(json!({"iss": "https://oidc.fly.io/other-org"})),
            ),
        ),
        ("unknown kid", rsa.token("rsa-2", &claims(json!({})))),
        ("alg none", with_alg("none")),
        ("alg HS256", with_alg("HS256")),
    ] {
        assert_eq!(
            status(token).await,
            reqwest::StatusCode::UNAUTHORIZED,
            "{rejected}"
        );
    }

    // An unknown key must not block cached keys while the issuer is slow.
    tokio::time::sleep(crate::server::auth::oidc::MIN_REFRESH_INTERVAL).await;
    pause.next.store(true, Ordering::Relaxed);
    let refresh_client = server.client.clone();
    let refresh_url = url.clone();
    let unknown_key_token = rsa.token("rsa-2", &claims(json!({})));
    let refresh_request = tokio::spawn(async move {
        refresh_client
            .get(refresh_url)
            .bearer_auth(unknown_key_token)
            .send()
            .await
            .unwrap()
            .status()
    });
    tokio::time::timeout(Duration::from_secs(5), pause.started.notified())
        .await
        .expect("JWKS refresh did not start");
    let cached_status = tokio::time::timeout(
        Duration::from_secs(1),
        status(rsa.token("rsa-1", &claims(json!({})))),
    )
    .await;
    pause.resume.notify_one();
    assert_eq!(
        refresh_request.await.unwrap(),
        reqwest::StatusCode::UNAUTHORIZED
    );
    assert!(
        cached_status
            .expect("cached key verification blocked on JWKS refresh")
            .is_success()
    );

    // Key rotation: a new `kid` triggers a JWKS refresh once the rate limit allows.
    let rng = SystemRandom::new();
    let ed25519 = Signer::Ed25519(
        ring_sig::Ed25519KeyPair::from_pkcs8(
            ring_sig::Ed25519KeyPair::generate_pkcs8(&rng)
                .unwrap()
                .as_ref(),
        )
        .unwrap(),
    );
    jwks.lock().unwrap()["keys"]
        .as_array_mut()
        .unwrap()
        .push(ed25519.jwk("ed-1"));
    tokio::time::sleep(crate::server::auth::oidc::MIN_REFRESH_INTERVAL).await;
    assert!(
        status(ed25519.token("ed-1", &claims(json!({}))))
            .await
            .is_success()
    );

    let token = crate::api::generate_token();
    let hash = crate::api::token_hash(&token);
    let config_path = server.tmp_dir.path().join("server.toml");
    // Changing trust on disk must leave the existing verifier and cached keys intact.
    super::token_reload::replace(&config_path, &format!("api.token_hashes = ['{hash}']"));
    super::token_reload::wait_for_set(&server, &rsa.token("rsa-1", &claims(json!({}))), &[hash])
        .await;
    assert!(
        status(ed25519.token("ed-1", &claims(json!({}))))
            .await
            .is_success()
    );
    super::token_reload::replace(&config_path, "api.token_hashes = []");
    super::token_reload::wait_for_set(&server, &ed25519.token("ed-1", &claims(json!({}))), &[])
        .await;
    assert_eq!(status(token).await, reqwest::StatusCode::UNAUTHORIZED);

    server.shutdown().await;
}
