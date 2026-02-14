//! A [`Fetcher`] implementation that uses `wstd::http` (WASIp2 HTTP) as the backend.

use boa_engine::{Context, JsNativeError, JsResult, JsString};
use boa_gc::{Finalize, Trace};
use boa_runtime::fetch::{Fetcher, request::JsRequest, response::JsResponse};
use std::cell::RefCell;
use std::rc::Rc;
use wstd::http::{Body, Client, Request};

/// Fetcher backed by WASIp2 HTTP outgoing requests via `wstd`.
#[derive(Debug, Clone, Default, Trace, Finalize, boa_engine::JsData)]
pub struct WasiFetcher;

/// Convert an anyhow error (from wstd) into a JsResult error.
fn to_js_err(err: impl std::fmt::Display) -> boa_engine::JsError {
    JsNativeError::error().with_message(err.to_string()).into()
}

impl Fetcher for WasiFetcher {
    async fn fetch(
        self: Rc<Self>,
        request: JsRequest,
        _context: &RefCell<&mut Context>,
    ) -> JsResult<JsResponse> {
        let http_req = request.into_inner(); // http::Request<Vec<u8>>
        let uri = http_req.uri().to_string();

        // Build wstd request, preserving method, headers, and body
        let mut builder = Request::builder()
            .method(http_req.method().as_str())
            .uri(&uri);
        for (name, value) in http_req.headers() {
            builder = builder.header(name.as_str(), value.to_str().map_err(to_js_err)?);
        }
        let body_bytes = http_req.into_body();
        let wstd_req = builder.body(Body::from(body_bytes)).map_err(to_js_err)?;

        // Send via WASIp2 HTTP
        let response = Client::new().send(wstd_req).await.map_err(to_js_err)?;

        // Read status + headers, then body
        let status = response.status().as_u16();
        let headers = response.headers().clone();
        let mut response_body = response.into_body();
        let body_bytes = Vec::from(response_body.contents().await.map_err(to_js_err)?);

        // Build http::Response and convert to JsResponse
        let mut http_resp_builder = http::Response::builder().status(status);
        for (name, value) in headers.iter() {
            http_resp_builder =
                http_resp_builder.header(name.as_str(), value.to_str().map_err(to_js_err)?);
        }
        let http_resp = http_resp_builder.body(body_bytes).map_err(to_js_err)?;

        Ok(JsResponse::basic(JsString::from(uri), http_resp))
    }
}
