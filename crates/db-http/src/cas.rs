use crate::{HttpPool, REQUEST_TIMEOUT, error::WireError};
use async_trait::async_trait;
use concepts::{
    ContentDigest,
    cas::{Cas, CasError},
};
use reqwest::{Method, Response, StatusCode};
use secrecy::ExposeSecret;

impl HttpPool {
    async fn blob_request(
        &self,
        method: Method,
        path: &str,
        body: Vec<u8>,
    ) -> Result<Response, CasError> {
        let mut closed = self.closed.subscribe();
        if *closed.borrow() {
            return Err(WireError::Closed.into());
        }
        let url = self
            .endpoint
            .join(path)
            .map_err(|err| CasError::Uncategorized(err.to_string()))?;
        let request = self
            .client
            .request(method, url)
            .bearer_auth(self.token.expose_secret())
            .timeout(REQUEST_TIMEOUT)
            .body(body);
        tokio::select! {
            result = request.send() => result.map_err(|err| CasError::Uncategorized(err.to_string())),
            _ = closed.changed() => Err(WireError::Closed.into()),
        }
    }
}

#[async_trait]
impl Cas for HttpPool {
    async fn read_blob(&self, digest: &ContentDigest) -> Result<Option<Vec<u8>>, CasError> {
        let response = self
            .blob_request(Method::GET, &format!("v1/blobs/{digest}"), Vec::new())
            .await?;
        match response.status() {
            StatusCode::OK => Ok(Some(
                response
                    .bytes()
                    .await
                    .map_err(|err| CasError::Uncategorized(err.to_string()))?
                    .to_vec(),
            )),
            StatusCode::NOT_FOUND => Ok(None),
            status => Err(CasError::Uncategorized(format!(
                "storage HTTP status {status}"
            ))),
        }
    }
    async fn write_blob(&self, content: &[u8]) -> Result<ContentDigest, CasError> {
        let response = self
            .blob_request(Method::POST, "v1/blobs", content.to_vec())
            .await?;
        if !response.status().is_success() {
            return Err(CasError::Uncategorized(format!(
                "storage HTTP status {}",
                response.status()
            )));
        }
        response
            .json::<Result<ContentDigest, WireError>>()
            .await
            .map_err(|err| CasError::Uncategorized(err.to_string()))?
            .map_err(CasError::from)
    }
    async fn contains_blob(&self, digest: &ContentDigest) -> Result<bool, CasError> {
        let response = self
            .blob_request(Method::HEAD, &format!("v1/blobs/{digest}"), Vec::new())
            .await?;
        match response.status() {
            StatusCode::OK => Ok(true),
            StatusCode::NOT_FOUND => Ok(false),
            status => Err(CasError::Uncategorized(format!(
                "storage HTTP status {status}"
            ))),
        }
    }
}
