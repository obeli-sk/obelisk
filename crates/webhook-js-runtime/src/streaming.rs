//! Streamed response and fetch bodies.
//!
//! Boa's `Response` holds a buffered body, so a JS shim adds `ReadableStream`, lets
//! `new Response(stream, init)` carry one, and replaces `fetch` with a version whose body
//! stays in the host until read. A streamed response sends its headers through the
//! responder channel, then the shim pumps the stream into the body channel.

use boa_engine::class::Class;
use boa_engine::object::builtins::{JsArray, JsFunction, JsUint8Array};
use boa_engine::{
    Context, JsArgs, JsNativeError, JsObject, JsResult, JsString, JsValue, NativeFunction, Source,
    js_string,
};
use boa_runtime::fetch::request::JsRequest;
use boa_runtime::fetch::response::JsResponse;
use futures::SinkExt as _;
use futures::channel::{mpsc, oneshot};
use std::cell::RefCell;
use std::collections::HashMap;
use std::io;
use wstd::http::body::{Body, Bytes, util::BodyExt as _, util::combinators::UnsyncBoxBody};
use wstd::http::{Client, Request, Response, StatusCode};

/// Chunks buffered between the JS pump and the outgoing HTTP body.
const BODY_CHANNEL_CAPACITY: usize = 8;

type BodyChunk = Result<Vec<u8>, io::Error>;

thread_local! {
    static RESPONDER: RefCell<Option<oneshot::Sender<Response<Body>>>> = const { RefCell::new(None) };
    static BODY_TX: RefCell<Option<mpsc::Sender<BodyChunk>>> = const { RefCell::new(None) };
    static FETCH_BODIES: RefCell<FetchBodies> = RefCell::new(FetchBodies::default());
}

#[derive(Default)]
struct FetchBodies {
    next_rid: u32,
    bodies: HashMap<u32, UnsyncBoxBody<Bytes, wstd::http::Error>>,
}

/// Set the channel that delivers the response head to the HTTP responder.
pub(crate) fn install_responder(sender: oneshot::Sender<Response<Body>>) {
    RESPONDER.with_borrow_mut(|responder| *responder = Some(sender));
}

/// Send the response unless one was sent already. Returns false if streaming already started.
pub(crate) fn respond(response: Response<Body>) -> bool {
    match RESPONDER.with_borrow_mut(Option::take) {
        Some(sender) => {
            // A dropped receiver means the responder is gone; nothing else can be done.
            let _ = sender.send(response);
            true
        }
        None => false,
    }
}

/// End a streamed body with an error, so the client sees a truncated response.
pub(crate) async fn abort_body(reason: String) {
    if let Some(mut sender) = BODY_TX.with_borrow_mut(Option::take) {
        let _ = sender.send(Err(io::Error::other(reason))).await;
    }
}

/// Build the shim and return its `serve(handler, request)` function.
pub(crate) fn setup(context: &mut Context) -> JsResult<JsFunction> {
    let native = JsObject::default(context.intrinsics());
    let sync_fns: [(JsString, NativeFunction); 3] = [
        (
            js_string!("respond"),
            NativeFunction::from_fn_ptr(respond_stream),
        ),
        (js_string!("close"), NativeFunction::from_fn_ptr(body_close)),
        (
            js_string!("fetchClose"),
            NativeFunction::from_fn_ptr(fetch_close),
        ),
    ];
    let async_fns: [(JsString, NativeFunction); 5] = [
        (
            js_string!("write"),
            NativeFunction::from_async_fn(body_write),
        ),
        (
            js_string!("abort"),
            NativeFunction::from_async_fn(body_abort),
        ),
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
    ];
    for (name, function) in sync_fns.into_iter().chain(async_fns) {
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
        .ok_or_else(|| JsNativeError::typ().with_message("streaming shim must be a function"))?;
    let serve = shim.call(&JsValue::undefined(), &[native.into()], context)?;
    serve
        .as_object()
        .and_then(|serve| JsFunction::from_object(serve.clone()))
        .ok_or_else(|| {
            JsNativeError::typ()
                .with_message("streaming shim must return a function")
                .into()
        })
}

/// `respond(response)`: send the head of a streamed `Response` and open the body channel.
fn respond_stream(_this: &JsValue, args: &[JsValue], _context: &mut Context) -> JsResult<JsValue> {
    let (status, headers) = {
        let object = args.get_or_undefined(0).as_object();
        let response = object
            .as_ref()
            .and_then(|object| object.downcast_ref::<JsResponse>())
            .ok_or_else(|| JsNativeError::typ().with_message("expected a Response"))?;
        let headers = response.headers().as_header_map().borrow().clone();
        (response.status(), headers)
    };
    let (sender, receiver) = mpsc::channel(BODY_CHANNEL_CAPACITY - 1);
    let mut response = Response::new(Body::from_try_stream(receiver));
    *response.status_mut() = StatusCode::from_u16(status).unwrap_or(StatusCode::OK);
    *response.headers_mut() = headers;
    if !respond(response) {
        return Err(JsNativeError::error()
            .with_message("response was already sent")
            .into());
    }
    BODY_TX.with_borrow_mut(|tx| *tx = Some(sender));
    Ok(JsValue::undefined())
}

/// `write(bytes)`: resolves to false once the client is gone.
async fn body_write(
    _this: &JsValue,
    args: &[JsValue],
    context: &RefCell<&mut Context>,
) -> JsResult<JsValue> {
    let bytes = uint8_array_bytes(args.get_or_undefined(0), &mut context.borrow_mut())?;
    let Some(mut sender) = BODY_TX.with_borrow(Clone::clone) else {
        return Ok(false.into());
    };
    Ok(sender.send(Ok(bytes)).await.is_ok().into())
}

/// `close()`: end the body cleanly.
fn body_close(_this: &JsValue, _args: &[JsValue], _context: &mut Context) -> JsResult<JsValue> {
    BODY_TX.with_borrow_mut(Option::take);
    Ok(JsValue::undefined())
}

/// `abort(reason)`: end the body with an error.
async fn body_abort(
    _this: &JsValue,
    args: &[JsValue],
    _context: &RefCell<&mut Context>,
) -> JsResult<JsValue> {
    let reason = args
        .get_or_undefined(0)
        .as_string()
        .map(|reason| reason.to_std_string_escaped())
        .unwrap_or_default();
    abort_body(reason).await;
    Ok(JsValue::undefined())
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

fn uint8_array_bytes(value: &JsValue, context: &mut Context) -> JsResult<Vec<u8>> {
    let array = value
        .as_object()
        .and_then(|object| JsUint8Array::from_object(object.clone()).ok())
        .ok_or_else(|| JsNativeError::typ().with_message("expected a Uint8Array"))?;
    array.to_vec(context)
}

fn to_js_err(err: impl std::fmt::Display) -> boa_engine::JsError {
    JsNativeError::error().with_message(err.to_string()).into()
}

const SHIM: &str = include_str!("streaming.js");
