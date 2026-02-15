//! A [`Fetcher`] implementation backed by WASIp2 HTTP.
//!
//! We build the outgoing WASI request manually instead of using
//! `wstd::http::Client::send` because `wstd` calls `.unwrap()` on
//! `outgoing_handler::handle()`, which traps on errors such as
//! `ErrorCode::HttpRequestDenied`. By calling `handle()` ourselves we
//! can surface the error as a rejected JS promise.

use boa_engine::{Context, JsNativeError, JsResult, JsString};
use boa_gc::{Finalize, Trace};
use boa_runtime::fetch::{Fetcher, request::JsRequest, response::JsResponse};
use std::cell::RefCell;
use std::rc::Rc;
use wasip2::http::outgoing_handler::OutgoingRequest;
use wasip2::http::types as wasi_http;
use wstd::io::{AsyncOutputStream, AsyncPollable};

/// Fetcher backed by WASIp2 HTTP outgoing requests.
#[derive(Debug, Clone, Default, Trace, Finalize, boa_engine::JsData)]
pub struct WasiFetcher;

fn to_js_err(err: impl std::fmt::Display) -> boa_engine::JsError {
    JsNativeError::error().with_message(err.to_string()).into()
}

/// Convert an `http::Method` to a WASI method.
fn to_wasi_method(m: &http::Method) -> wasi_http::Method {
    match *m {
        http::Method::GET => wasi_http::Method::Get,
        http::Method::HEAD => wasi_http::Method::Head,
        http::Method::POST => wasi_http::Method::Post,
        http::Method::PUT => wasi_http::Method::Put,
        http::Method::DELETE => wasi_http::Method::Delete,
        http::Method::CONNECT => wasi_http::Method::Connect,
        http::Method::OPTIONS => wasi_http::Method::Options,
        http::Method::TRACE => wasi_http::Method::Trace,
        http::Method::PATCH => wasi_http::Method::Patch,
        ref other => wasi_http::Method::Other(other.as_str().to_owned()),
    }
}

/// Build a WASI `OutgoingRequest` from an `http::Request<Vec<u8>>`,
/// returning the request handle and the body bytes.
fn build_outgoing_request(req: http::Request<Vec<u8>>) -> JsResult<(OutgoingRequest, Vec<u8>)> {
    let (parts, body) = req.into_parts();

    // Headers
    let fields = wasi_http::Fields::new();
    for (name, value) in &parts.headers {
        fields
            .append(name.as_str(), value.as_bytes())
            .map_err(|e| to_js_err(format!("invalid header `{name}`: {e:?}")))?
    }

    let wasi_req = OutgoingRequest::new(fields);

    wasi_req
        .set_method(&to_wasi_method(&parts.method))
        .map_err(|()| to_js_err("method rejected by wasi-http"))?;

    let scheme = parts
        .uri
        .scheme()
        .map(|s| match s.as_str() {
            "http" => wasi_http::Scheme::Http,
            "https" => wasi_http::Scheme::Https,
            other => wasi_http::Scheme::Other(other.to_owned()),
        })
        .unwrap_or(wasi_http::Scheme::Https);
    wasi_req
        .set_scheme(Some(&scheme))
        .map_err(|()| to_js_err("scheme rejected by wasi-http"))?;

    if let Some(authority) = parts.uri.authority() {
        wasi_req
            .set_authority(Some(authority.as_str()))
            .map_err(|()| to_js_err("authority rejected by wasi-http"))?;
    }

    if let Some(pq) = parts.uri.path_and_query() {
        wasi_req
            .set_path_with_query(Some(pq.as_str()))
            .map_err(|()| to_js_err("path rejected by wasi-http"))?;
    }

    Ok((wasi_req, body))
}

impl Fetcher for WasiFetcher {
    async fn fetch(
        self: Rc<Self>,
        request: JsRequest,
        _context: &RefCell<&mut Context>,
    ) -> JsResult<JsResponse> {
        let http_req = request.into_inner();
        let uri = http_req.uri().to_string();

        let (wasi_req, body_bytes) = build_outgoing_request(http_req)?;

        // Get the outgoing body stream *before* calling handle
        let wasi_body = wasi_req
            .body()
            .map_err(|_| to_js_err("failed to get outgoing body"))?;

        // Start the request — this is where errors like HttpRequestDenied surface
        let future_resp = wasip2::http::outgoing_handler::handle(wasi_req, None)
            .map_err(|e| to_js_err(format!("HTTP request denied: {e:?}")))?;

        // Send body and receive response concurrently
        let ((), response) = futures_lite::future::try_zip(
            async {
                // Write body bytes to the outgoing stream
                let out_stream = wasi_body
                    .write()
                    .map_err(|_| to_js_err("failed to get outgoing body stream"))?;
                let out_stream = AsyncOutputStream::new(out_stream);
                out_stream
                    .write_all(&body_bytes)
                    .await
                    .map_err(|e| to_js_err(format!("write error: {e:?}")))?;
                drop(out_stream);
                wasip2::http::types::OutgoingBody::finish(wasi_body, None)
                    .map_err(|e| to_js_err(format!("finish body error: {e:?}")))?;
                Ok(())
            },
            async {
                // Wait for the response
                AsyncPollable::new(future_resp.subscribe()).wait_for().await;
                let incoming = future_resp
                    .get()
                    .ok_or_else(|| to_js_err("response not ready"))?
                    .map_err(|_| to_js_err("response already consumed"))?
                    .map_err(|e| to_js_err(format!("HTTP error: {e:?}")))?;

                // Read status and headers
                let status = incoming.status();
                let wasi_headers = incoming.headers();
                let header_entries = wasi_headers.entries();

                // Read body
                let incoming_body = incoming
                    .consume()
                    .map_err(|_| to_js_err("failed to consume response body"))?;
                let body_stream = incoming_body
                    .stream()
                    .map_err(|_| to_js_err("failed to get response body stream"))?;

                let mut body_bytes = Vec::new();
                loop {
                    // Poll for readiness
                    let pollable = body_stream.subscribe();
                    AsyncPollable::new(pollable).wait_for().await;

                    match body_stream.read(64 * 1024) {
                        Ok(chunk) => body_bytes.extend_from_slice(&chunk),
                        Err(wasip2::io::streams::StreamError::Closed) => break,
                        Err(e) => return Err(to_js_err(format!("read error: {e:?}"))),
                    }
                }
                drop(body_stream);
                drop(incoming_body);

                // Build http::Response
                let mut builder = http::Response::builder().status(status);
                for (name, value) in &header_entries {
                    builder = builder.header(
                        name.as_str(),
                        http::HeaderValue::from_bytes(value)
                            .map_err(|e| to_js_err(format!("invalid response header: {e}")))?,
                    );
                }
                builder.body(body_bytes).map_err(to_js_err)
            },
        )
        .await?;

        Ok(JsResponse::basic(JsString::from(uri), response))
    }
}
