use secrecy::{ExposeSecret as _, SecretString};
use serde::de::DeserializeOwned;

/// Holds the API token and builds web-API clients that inject it.
#[derive(Clone, Default)]
pub(crate) struct ClientStartup {
    api_token: Option<SecretString>,
    api_url: String,
    cloud: bool,
    verbose: bool,
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
    pub(crate) fn new(api_url: String, flag: Option<SecretString>) -> Self {
        let api_token = flag.filter(|token| !token.expose_secret().is_empty());

        Self {
            api_token,
            api_url,
            cloud: false,
            verbose: false,
        }
    }

    pub(crate) fn cloud(api_url: String, api_token: Option<SecretString>, verbose: bool) -> Self {
        Self {
            api_token,
            api_url,
            cloud: true,
            verbose,
        }
    }

    pub(crate) fn api_url(&self) -> &str {
        &self.api_url
    }

    pub(crate) fn detailed_output(&self) -> bool {
        !self.cloud || self.verbose
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
        let builder = reqwest::Client::builder().default_headers(headers);
        Ok(if self.cloud {
            builder.redirect(reqwest::redirect::Policy::none())
        } else {
            builder
        })
    }
}
