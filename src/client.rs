use crate::config::secret_registry::{API_TOKEN_CLIENT, API_TOKEN_SERVER};
use secrecy::{ExposeSecret as _, SecretString};
use serde::de::DeserializeOwned;

/// Holds the API token (resolved from `--api-token` > `OBELISK_API_TOKEN` >
/// `OBELISK__API__TOKEN`) and builds web-API clients that inject it.
#[derive(Clone, Default)]
pub(crate) struct ClientStartup {
    api_token: Option<SecretString>,
}

pub(crate) async fn send_json<T: DeserializeOwned>(
    request: reqwest::RequestBuilder,
) -> Result<T, anyhow::Error> {
    let response = request.send().await?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("server returned {status}: {body}");
    }
    Ok(response.json().await?)
}

pub(crate) async fn send_empty(request: reqwest::RequestBuilder) -> Result<(), anyhow::Error> {
    let response = request.send().await?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("server returned {status}: {body}");
    }
    Ok(())
}

pub(crate) async fn send_bytes(
    request: reqwest::RequestBuilder,
) -> Result<bytes::Bytes, anyhow::Error> {
    let response = request.send().await?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("server returned {status}: {body}");
    }
    Ok(response.bytes().await?)
}

impl ClientStartup {
    pub(crate) fn new(flag: Option<SecretString>) -> Self {
        let api_token = flag
            .or_else(|| std::env::var(API_TOKEN_CLIENT).ok().map(SecretString::from))
            .or_else(|| std::env::var(API_TOKEN_SERVER).ok().map(SecretString::from))
            .filter(|token| !token.expose_secret().is_empty());

        Self { api_token }
    }

    /// HTTP client for web-API calls, presenting the API token as a default header.
    pub(crate) fn web_api_client(&self) -> Result<reqwest::Client, anyhow::Error> {
        Ok(self.web_api_client_builder()?.build()?)
    }

    pub(crate) fn web_api_client_builder(&self) -> Result<reqwest::ClientBuilder, anyhow::Error> {
        let mut headers = reqwest::header::HeaderMap::new();
        if let Some(token) = &self.api_token {
            let mut value = reqwest::header::HeaderValue::from_str(&format!(
                "Bearer {}",
                token.expose_secret()
            ))
            .map_err(|_| anyhow::anyhow!("API token contains invalid header characters"))?;
            value.set_sensitive(true);
            headers.insert(reqwest::header::AUTHORIZATION, value);
        }
        Ok(reqwest::Client::builder().default_headers(headers))
    }
}
