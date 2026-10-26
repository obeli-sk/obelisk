//! Streamed response bodies.
//!
//! `boa_common::fetch_stream` lets `new Response(stream, init)` carry a `ReadableStream`.
//! A streamed response sends its headers through the responder channel, then the shim
//! pumps the stream into the body channel.

use boa_engine::object::builtins::{JsFunction, JsUint8Array};
use boa_engine::{
    Context, JsArgs, JsNativeError, JsObject, JsResult, JsString, JsValue, NativeFunction, Source,
    js_string,
};
use boa_runtime::fetch::response::JsResponse;
use futures::SinkExt as _;
use futures::channel::{mpsc, oneshot};
use std::cell::RefCell;
use std::io;
use wstd::http::body::Body;
use wstd::http::{Response, StatusCode};

/// Chunks buffered between the JS pump and the outgoing HTTP body.
const BODY_CHANNEL_CAPACITY: usize = 8;

type BodyChunk = Result<Vec<u8>, io::Error>;

thread_local! {
    static RESPONDER: RefCell<Option<oneshot::Sender<Response<Body>>>> = const { RefCell::new(None) };
    static BODY_TX: RefCell<Option<mpsc::Sender<BodyChunk>>> = const { RefCell::new(None) };
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

/// Set up `fetch` and the shim, and return the shim's `serve(handler, request)` function.
pub(crate) fn setup(context: &mut Context) -> JsResult<JsFunction> {
    let helpers = boa_common::fetch_stream::setup(context)?;
    let native = JsObject::default(context.intrinsics());
    let functions: [(JsString, NativeFunction); 4] = [
        (
            js_string!("respond"),
            NativeFunction::from_fn_ptr(respond_stream),
        ),
        (js_string!("close"), NativeFunction::from_fn_ptr(body_close)),
        (
            js_string!("write"),
            NativeFunction::from_async_fn(body_write),
        ),
        (
            js_string!("abort"),
            NativeFunction::from_async_fn(body_abort),
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
        .ok_or_else(|| JsNativeError::typ().with_message("streaming shim must be a function"))?;
    let serve = shim.call(
        &JsValue::undefined(),
        &[native.into(), helpers.into()],
        context,
    )?;
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

fn uint8_array_bytes(value: &JsValue, context: &mut Context) -> JsResult<Vec<u8>> {
    let array = value
        .as_object()
        .and_then(|object| JsUint8Array::from_object(object.clone()).ok())
        .ok_or_else(|| JsNativeError::typ().with_message("expected a Uint8Array"))?;
    array.to_vec(context)
}

const SHIM: &str = include_str!("streaming.js");
