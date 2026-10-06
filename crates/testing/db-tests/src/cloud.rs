use reqwest::{Client, Method};
use secrecy::{ExposeSecret as _, SecretString};
use serde_json::{Value, json};
use std::time::Duration;

pub struct CloudDatabase {
    client: Client,
    base: String,
    token: SecretString,
    name: String,
}

impl CloudDatabase {
    async fn request(&self, method: Method, path: &str, body: Value) -> Result<Value, String> {
        let response = self
            .client
            .request(method, format!("{}{path}", self.base))
            .bearer_auth(self.token.expose_secret())
            .json(&body)
            .send()
            .await
            .map_err(|error| {
                format!(
                    "Cloud database management request failed: {}",
                    error.without_url()
                )
            })?;
        let status = response.status();
        if !status.is_success() {
            return Err(format!(
                "Cloud database management request returned {status}"
            ));
        }
        let bytes = response
            .bytes()
            .await
            .map_err(|_| "cannot read management response")?;
        if bytes.is_empty() {
            Ok(Value::Null)
        } else {
            serde_json::from_slice(&bytes)
                .map_err(|_| "cannot decode management response".to_owned())
        }
    }

    pub async fn create(token_path: &str) -> (Self, String, SecretString) {
        let organization = super::get_env_val("TEST_TURSO_ORGANIZATION");
        let group = super::get_env_val("TEST_TURSO_GROUP");
        let name = format!(
            "obelisk-test-{}",
            concepts::prefixed_ulid::DeploymentId::generate()
        )
        .to_lowercase()
        .replace('_', "-");
        let tls = rustls::ClientConfig::builder_with_provider(std::sync::Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(rustls::RootCertStore::from_iter(
            webpki_roots::TLS_SERVER_ROOTS.iter().cloned(),
        ))
        .with_no_client_auth();
        let database = Self {
            client: Client::builder()
                .tls_backend_preconfigured(tls)
                .timeout(Duration::from_secs(60))
                .build()
                .unwrap(),
            base: format!("https://api.turso.tech/v1/organizations/{organization}"),
            token: SecretString::from(
                std::fs::read_to_string(token_path)
                    .expect("read Platform token")
                    .trim()
                    .to_owned(),
            ),
            name,
        };
        let created = database
            .request(
                Method::POST,
                "/databases",
                json!({"name":database.name,"group":group,"use_tursodb":true}),
            )
            .await
            .expect("create isolated Cloud test database");
        let credentials = async {
            let host = created["database"]["Hostname"]
                .as_str()
                .ok_or("missing database hostname")?;
            let auth = database
                .request(
                    Method::POST,
                    &format!(
                        "/databases/{}/auth/tokens?expiration=1h&authorization=full-access",
                        database.name
                    ),
                    json!({}),
                )
                .await?;
            let token = auth["jwt"].as_str().ok_or("missing SQL token")?.to_owned();
            Ok::<_, String>((format!("turso://{host}"), SecretString::from(token)))
        }
        .await;
        match credentials {
            Ok((url, token)) => {
                let ready = database.wait_for_sql(&url, &token).await;
                if let Err(error) = ready {
                    database.delete().await;
                    panic!("{error}");
                }
                (database, url, token)
            }
            Err(error) => {
                database.delete().await;
                panic!("{error}");
            }
        }
    }

    async fn wait_for_sql(&self, url: &str, token: &SecretString) -> Result<(), String> {
        let endpoint = format!("{}/v3/pipeline", url.replace("turso://", "https://"));
        for _ in 0..12 {
            let response = self.client.post(&endpoint).bearer_auth(token.expose_secret())
                .timeout(Duration::from_secs(5))
                .json(&json!({"requests":[{"type":"execute","stmt":{"sql":"SELECT 1","args":[],"want_rows":false}},{"type":"close"}]}))
                .send().await;
            if let Ok(response) = response {
                if response.status() == reqwest::StatusCode::UNAUTHORIZED
                    || response.status() == reqwest::StatusCode::FORBIDDEN
                {
                    return Err("SQL readiness authentication failed".to_owned());
                }
                if response.status().is_success()
                    && let Ok(body) = response.json::<Value>().await
                    && body["results"][0]["type"] == "ok"
                {
                    return Ok(());
                }
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        Err("new Cloud database did not become SQL-ready".to_owned())
    }

    pub async fn delete(&self) {
        self.request(
            Method::DELETE,
            &format!("/databases/{}", self.name),
            json!({}),
        )
        .await
        .expect("delete isolated Cloud test database");
    }
}
