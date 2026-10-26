use secrecy::{ExposeSecret as _, SecretString};
use serde::de::DeserializeOwned;

pub(crate) const USER_AGENT: &str =
    const_format::concatcp!("obelisk/", crate::args::shadow::PKG_VERSION);

/// Holds the API token and builds web-API clients that inject it.
#[derive(Clone, Default)]
pub(crate) struct ClientStartup {
    api_token: Option<SecretString>,
    api_url: String,
    cloud: bool,
    verbose: bool,
}

static TRACE: std::sync::Mutex<Option<std::time::Instant>> = std::sync::Mutex::new(None);

/// Enables `--verbose` step tracing on stderr.
pub(crate) fn enable_trace() {
    *TRACE.lock().unwrap() = Some(std::time::Instant::now());
}

/// Prints a verbose step prefixed with the time elapsed since the previous step.
pub(crate) fn trace(message: impl std::fmt::Display) {
    print_step(message, false);
}

/// Prints a progress step, prefixed with the elapsed time when tracing.
pub(crate) fn step(message: impl std::fmt::Display) {
    print_step(message, true);
}

fn print_step(message: impl std::fmt::Display, always: bool) {
    if let Some(last) = TRACE.lock().unwrap().as_mut() {
        let now = std::time::Instant::now();
        eprintln!("[+{:.3}s] {message}", (now - *last).as_secs_f64());
        *last = now;
    } else if always {
        eprintln!("{message}");
    }
}

pub(crate) async fn send_json<T: DeserializeOwned>(
    request: RequestBuilder,
) -> Result<T, anyhow::Error> {
    let response = request.send().await?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("server returned {status}: {body}");
    }
    Ok(response.json().await?)
}

pub(crate) async fn send_empty(request: RequestBuilder) -> Result<(), anyhow::Error> {
    let response = request.send().await?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("server returned {status}: {body}");
    }
    Ok(())
}

pub(crate) async fn send_bytes(request: RequestBuilder) -> Result<bytes::Bytes, anyhow::Error> {
    let response = request.send().await?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("server returned {status}: {body}");
    }
    Ok(response.bytes().await?)
}

impl ClientStartup {
    pub(crate) fn new(api_url: String, flag: Option<SecretString>, verbose: bool) -> Self {
        let api_token = flag.filter(|token| !token.expose_secret().is_empty());

        Self {
            api_token,
            api_url,
            cloud: false,
            verbose,
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

    pub(crate) fn is_cloud(&self) -> bool {
        self.cloud
    }

    /// HTTP client for web-API calls, presenting the API token as a default header.
    pub(crate) fn web_api_client(&self) -> Result<HttpClient, anyhow::Error> {
        Ok(HttpClient::new(
            self.web_api_client_builder()?.build()?,
            self.verbose,
        ))
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
        let builder = reqwest::Client::builder()
            .user_agent(USER_AGENT)
            .default_headers(headers);
        Ok(if self.cloud {
            builder.redirect(reqwest::redirect::Policy::none())
        } else {
            builder
        })
    }
}

pub(crate) struct HttpClient {
    inner: reqwest::Client,
    verbose: bool,
}

impl HttpClient {
    pub(crate) fn new(inner: reqwest::Client, verbose: bool) -> Self {
        Self { inner, verbose }
    }

    pub(crate) fn request(
        &self,
        method: reqwest::Method,
        url: impl reqwest::IntoUrl,
    ) -> RequestBuilder {
        RequestBuilder {
            inner: self.inner.request(method, url),
            verbose: self.verbose,
        }
    }

    pub(crate) fn get(&self, url: impl reqwest::IntoUrl) -> RequestBuilder {
        self.request(reqwest::Method::GET, url)
    }

    pub(crate) fn post(&self, url: impl reqwest::IntoUrl) -> RequestBuilder {
        self.request(reqwest::Method::POST, url)
    }

    pub(crate) fn put(&self, url: impl reqwest::IntoUrl) -> RequestBuilder {
        self.request(reqwest::Method::PUT, url)
    }

    pub(crate) fn delete(&self, url: impl reqwest::IntoUrl) -> RequestBuilder {
        self.request(reqwest::Method::DELETE, url)
    }
}

pub(crate) struct RequestBuilder {
    inner: reqwest::RequestBuilder,
    verbose: bool,
}

impl RequestBuilder {
    pub(crate) fn header<K, V>(self, key: K, value: V) -> Self
    where
        reqwest::header::HeaderName: TryFrom<K>,
        <reqwest::header::HeaderName as TryFrom<K>>::Error: Into<http::Error>,
        reqwest::header::HeaderValue: TryFrom<V>,
        <reqwest::header::HeaderValue as TryFrom<V>>::Error: Into<http::Error>,
    {
        Self {
            inner: self.inner.header(key, value),
            ..self
        }
    }

    pub(crate) fn json<T: serde::Serialize + ?Sized>(self, body: &T) -> Self {
        Self {
            inner: self.inner.json(body),
            ..self
        }
    }

    pub(crate) fn query<T: serde::Serialize + ?Sized>(self, query: &T) -> Self {
        Self {
            inner: self.inner.query(query),
            ..self
        }
    }

    pub(crate) fn multipart(self, form: reqwest::multipart::Form) -> Self {
        Self {
            inner: self.inner.multipart(form),
            ..self
        }
    }

    pub(crate) async fn send(self) -> Result<reqwest::Response, reqwest::Error> {
        let (client, request) = self.inner.build_split();
        let request = request?;
        if self.verbose {
            step(format_args!(
                "> {} {}",
                request.method(),
                diagnostic_url(request.url())
            ));
        }
        let response = client.execute(request).await;
        let response = if self.verbose {
            response.map_err(reqwest::Error::without_url)
        } else {
            response
        };
        if self.verbose {
            match &response {
                Ok(response) => step(format_args!("< {}", response.status())),
                Err(err) => step(format_args!("< Request failed: {err}")),
            }
        }
        response
    }
}

fn diagnostic_url(url: &reqwest::Url) -> reqwest::Url {
    let mut url = url.clone();
    let _ = url.set_username("");
    let _ = url.set_password(None);
    url.set_fragment(None);
    if url.query().is_some() {
        let pairs: Vec<_> = url
            .query_pairs()
            .map(|(key, value)| {
                let name = key.to_ascii_lowercase();
                let sensitive = [
                    "token",
                    "secret",
                    "password",
                    "credential",
                    "signature",
                    "key",
                    "auth",
                ]
                .iter()
                .any(|part| name.contains(part));
                (
                    key.into_owned(),
                    if sensitive {
                        "[REDACTED]".to_owned()
                    } else {
                        value.into_owned()
                    },
                )
            })
            .collect();
        url.query_pairs_mut().clear().extend_pairs(pairs);
    }
    url
}

#[cfg(test)]
mod tests {
    use super::diagnostic_url;

    #[test]
    fn diagnostic_url_redacts_credentials_and_preserves_request_parameters() {
        let original = reqwest::Url::parse(
            "https://user:password@example.com/prefix/v1/executions?follow=true&api%5Ftoken=first&api_token=second&X-Amz-Signature=signed&password=hidden&cursor=a%2Bb#secret",
        ).unwrap();
        let redacted = diagnostic_url(&original);
        assert_eq!(redacted.username(), "");
        assert_eq!(redacted.password(), None);
        assert_eq!(redacted.fragment(), None);
        assert_eq!(redacted.path(), "/prefix/v1/executions");
        assert_eq!(
            redacted.query_pairs().collect::<Vec<_>>(),
            vec![
                ("follow".into(), "true".into()),
                ("api_token".into(), "[REDACTED]".into()),
                ("api_token".into(), "[REDACTED]".into()),
                ("X-Amz-Signature".into(), "[REDACTED]".into()),
                ("password".into(), "[REDACTED]".into()),
                ("cursor".into(), "a+b".into()),
            ]
        );
        assert_eq!(original.username(), "user");
        assert_eq!(original.query_pairs().nth(1).unwrap().1, "first");
    }
}
