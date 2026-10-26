//! `fetch` with a streamed response body, shared by Boa activities and webhooks.
//!
//! Boa's `Response` holds a buffered body, so a JS shim adds `ReadableStream`, lets
//! `new Response(stream, init)` carry one, and replaces `fetch` with a version that
//! resolves once the headers arrive and keeps the body in the host until read.

use crate::wasi_fetcher::WasiFetcher;
use boa_engine::class::Class;
use boa_engine::object::builtins::{JsArray, JsUint8Array};
use boa_engine::{
    Context, JsArgs, JsNativeError, JsObject, JsResult, JsString, JsValue, NativeFunction, Source,
    js_string,
};
use boa_runtime::extensions::FetchExtension;
use boa_runtime::fetch::request::JsRequest;
use boa_runtime::fetch::response::JsResponse;
use std::cell::RefCell;
use std::collections::HashMap;
use wstd::http::body::{Body, Bytes, util::BodyExt as _, util::combinators::UnsyncBoxBody};
use wstd::http::{Client, Request};

thread_local! {
    static FETCH_BODIES: RefCell<FetchBodies> = RefCell::new(FetchBodies::default());
}

#[derive(Default)]
struct FetchBodies {
    next_rid: u32,
    bodies: HashMap<u32, UnsyncBoxBody<Bytes, wstd::http::Error>>,
}

/// Register `fetch`, `Request`, `Response`, `ReadableStream` and a streaming `TextDecoder`.
/// Returns the shim's helpers (`toBytes`, `bodyStream`, `isStreamed`, `NativeResponse`).
pub fn setup(context: &mut Context) -> JsResult<JsObject> {
    boa_runtime::register(FetchExtension(WasiFetcher), None, context)?;
    let native = JsObject::default(context.intrinsics());
    let functions: [(JsString, NativeFunction); 4] = [
        (
            js_string!("fetch"),
            NativeFunction::from_async_fn(fetch_start),
        ),
        (
            js_string!("fetchRead"),
            NativeFunction::from_async_fn(fetch_read),
        ),
        (
            js_string!("fetchCollect"),
            NativeFunction::from_async_fn(fetch_collect),
        ),
        (
            js_string!("fetchClose"),
            NativeFunction::from_fn_ptr(fetch_close),
        ),
    ];
    for (name, function) in functions {
        native.set(
            name,
            function.to_js_function(context.realm()),
            false,
            context,
        )?;
    }
    let shim = context.eval(Source::from_bytes(SHIM))?;
    let shim = shim
        .as_callable()
        .ok_or_else(|| JsNativeError::typ().with_message("fetch shim must be a function"))?;
    shim.call(&JsValue::undefined(), &[native.into()], context)?
        .as_object()
        .ok_or_else(|| {
            JsNativeError::typ()
                .with_message("fetch shim must return an object")
                .into()
        })
}

/// `fetch(request)`: send the request and resolve to `[response, rid]` once headers arrive.
/// The returned `Response` has an empty body; the shim reads the real one through `rid`.
async fn fetch_start(
    _this: &JsValue,
    args: &[JsValue],
    context: &RefCell<&mut Context>,
) -> JsResult<JsValue> {
    let request = args
        .get_or_undefined(0)
        .as_object()
        .and_then(|object| {
            object
                .downcast_ref::<JsRequest>()
                .map(|request| request.clone())
        })
        .ok_or_else(|| JsNativeError::typ().with_message("expected a Request"))?;
    let http_request = request.into_inner(context).await?;
    let uri = http_request.uri().to_string();
    let (mut parts, body) = http_request.into_parts();
    if !parts.headers.contains_key(http::header::ACCEPT_LANGUAGE) {
        parts.headers.insert(
            http::header::ACCEPT_LANGUAGE,
            http::HeaderValue::from_static("en-US"),
        );
    }
    let body = body.map_or_else(Body::empty, Body::from);
    let response = Client::new()
        .send(Request::from_parts(parts, body))
        .await
        .map_err(to_js_err)?;
    let (parts, body) = response.into_parts();
    let rid = FETCH_BODIES.with_borrow_mut(|fetch_bodies| {
        let rid = fetch_bodies.next_rid;
        fetch_bodies.next_rid += 1;
        fetch_bodies.bodies.insert(rid, body.into_boxed_body());
        rid
    });
    let response = JsResponse::basic(
        JsString::from(uri),
        http::Response::from_parts(parts, Vec::new()),
    );
    let context = &mut context.borrow_mut();
    let response = JsResponse::from_data(response, context)?;
    Ok(JsArray::from_iter([response.into(), rid.into()], context).into())
}

/// `fetchRead(rid)`: resolve to the next chunk, or `undefined` at the end of the body.
async fn fetch_read(
    _this: &JsValue,
    args: &[JsValue],
    context: &RefCell<&mut Context>,
) -> JsResult<JsValue> {
    let rid = rid_arg(args)?;
    let mut body = take_fetch_body(rid)?;
    let chunk = next_data(&mut body).await;
    if matches!(chunk, Ok(Some(_))) {
        FETCH_BODIES.with_borrow_mut(|fetch_bodies| fetch_bodies.bodies.insert(rid, body));
    }
    match chunk? {
        Some(chunk) => Ok(JsUint8Array::from_iter(chunk, &mut context.borrow_mut())?.into()),
        None => Ok(JsValue::undefined()),
    }
}

/// `fetchCollect(rid, asText)`: read the rest of the body as a string or a `Uint8Array`.
async fn fetch_collect(
    _this: &JsValue,
    args: &[JsValue],
    context: &RefCell<&mut Context>,
) -> JsResult<JsValue> {
    let rid = rid_arg(args)?;
    let as_text = args.get_or_undefined(1).to_boolean();
    let mut body = take_fetch_body(rid)?;
    let mut bytes = Vec::new();
    while let Some(chunk) = next_data(&mut body).await? {
        bytes.extend_from_slice(&chunk);
    }
    if as_text {
        Ok(JsString::from(String::from_utf8_lossy(&bytes).as_ref()).into())
    } else {
        Ok(JsUint8Array::from_iter(bytes, &mut context.borrow_mut())?.into())
    }
}

/// `fetchClose(rid)`: drop an unread body.
fn fetch_close(_this: &JsValue, args: &[JsValue], _context: &mut Context) -> JsResult<JsValue> {
    let rid = rid_arg(args)?;
    FETCH_BODIES.with_borrow_mut(|fetch_bodies| fetch_bodies.bodies.remove(&rid));
    Ok(JsValue::undefined())
}

fn rid_arg(args: &[JsValue]) -> JsResult<u32> {
    args.get_or_undefined(0)
        .as_number()
        .filter(|rid| rid.fract() == 0.0 && *rid >= 0.0 && *rid <= f64::from(u32::MAX))
        .map(|rid| rid as u32)
        .ok_or_else(|| JsNativeError::typ().with_message("invalid body id").into())
}

/// Remove the body while it is read, so a concurrent read fails instead of interleaving.
fn take_fetch_body(rid: u32) -> JsResult<UnsyncBoxBody<Bytes, wstd::http::Error>> {
    FETCH_BODIES
        .with_borrow_mut(|fetch_bodies| fetch_bodies.bodies.remove(&rid))
        .ok_or_else(|| {
            JsNativeError::typ()
                .with_message("response body is closed or already being read")
                .into()
        })
}

/// Next data frame, skipping trailers.
async fn next_data(body: &mut UnsyncBoxBody<Bytes, wstd::http::Error>) -> JsResult<Option<Bytes>> {
    loop {
        match body.frame().await {
            Some(Ok(frame)) => {
                if let Ok(data) = frame.into_data() {
                    return Ok(Some(data));
                }
            }
            Some(Err(err)) => return Err(to_js_err(err)),
            None => return Ok(None),
        }
    }
}

fn to_js_err(err: impl std::fmt::Display) -> boa_engine::JsError {
    JsNativeError::error().with_message(err.to_string()).into()
}

const SHIM: &str = include_str!("fetch_stream.js");
