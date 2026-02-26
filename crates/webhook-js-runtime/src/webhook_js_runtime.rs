//! JavaScript webhook runtime using Boa JS engine.
//!
//! This is a WASI HTTP handler that runs JavaScript code to process HTTP requests.
//! The JS code is provided via the `JS_SOURCE` environment variable.
//!
//! The JS handler receives a request object and should return a response object:
//! ```js
//! function handle(request) {
//!     return {
//!         status: 200,
//!         headers: [["content-type", "text/plain"]],
//!         body: "Hello from JS!"
//!     };
//! }
//! ```
//!
//! Async handlers with fetch are also supported:
//! ```js
//! async function handle(request) {
//!     const resp = await fetch("https://example.com/api");
//!     const data = await resp.text();
//!     return { status: 200, headers: [], body: data };
//! }
//! ```

use crate::generated::obelisk::log::log;
use boa_common::console::{ObeliskLogger, json_stringify, setup_console};
use boa_common::wasi_fetcher::WasiFetcher;
use boa_common::wasi_job_executor::WasiJobExecutor;
use boa_engine::{
    Context, JsError, JsResult, JsValue, Source, builtins::promise::PromiseState,
    object::builtins::JsPromise,
};
use boa_runtime::extensions::FetchExtension;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use wstd::http::body::Body;
use wstd::http::{Request, Response, StatusCode};

/// Logger implementation using the generated obelisk:log bindings.
#[derive(Clone, Copy)]
struct Logger;

impl ObeliskLogger for Logger {
    fn trace(&self, msg: &str) {
        log::trace(msg);
    }
    fn debug(&self, msg: &str) {
        log::debug(msg);
    }
    fn info(&self, msg: &str) {
        log::info(msg);
    }
    fn warn(&self, msg: &str) {
        log::warn(msg);
    }
    fn error(&self, msg: &str) {
        log::error(msg);
    }
}

const FN_NAME: &str = "handle";

#[wstd::http_server]
async fn main(request: Request<Body>) -> Result<Response<Body>, wstd::http::Error> {
    // Get JS source from environment
    let js_source = match std::env::var("__OBELISK_JS_SOURCE__") {
        Ok(source) => source,
        Err(_) => {
            return Ok(Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::from(
                    "__OBELISK_JS_SOURCE__ environment variable not set",
                ))?);
        }
    };

    // Convert request to JSON for JS
    let request_json = request_to_json(&request);

    // Run JS and get response
    match run_js_handler_async(&js_source, &request_json).await {
        Ok(response_json) => json_to_response(&response_json),
        Err(err) => {
            log::error(&format!("JS error: {err}"));
            Ok(Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::from(format!("JS error: {err}")))?)
        }
    }
}

/// Convert HTTP request to JSON string for JS consumption.
fn request_to_json(request: &Request<Body>) -> String {
    let method = request.method().as_str();
    let uri = request.uri().to_string();

    let headers: HashMap<String, Vec<String>> =
        request
            .headers()
            .iter()
            .fold(HashMap::new(), |mut acc, (k, v)| {
                acc.entry(k.to_string())
                    .or_default()
                    .push(v.to_str().unwrap_or_default().to_string());
                acc
            });

    // For now, we don't read the body to keep it simple
    // TODO: Add body reading support
    let body = "";

    serde_json::json!({
        "method": method,
        "url": uri,
        "headers": headers,
        "body": body,
    })
    .to_string()
}

/// Run the JS handler function and return the response JSON (async version).
async fn run_js_handler_async(js_source: &str, request_json: &str) -> Result<String, String> {
    let executor = Rc::new(WasiJobExecutor::default());
    let mut context = Context::builder()
        .job_executor(executor.clone())
        .build()
        .expect("building context must work");

    // Set up console.log -> obelisk:log
    setup_console(&mut context, Logger).expect("console setup must work");

    // Set up fetch
    setup_fetch(&mut context).expect("fetch setup must work");

    // Parse the request JSON and make it available
    let request_js = format!("const __request__ = {request_json};");
    context
        .eval(Source::from_bytes(&request_js))
        .map_err(|e| format!("Failed to set request: {e}"))?;

    // Execute the user's JS code
    context
        .eval(Source::from_bytes(js_source))
        .map_err(|e| format!("Failed to execute JS: {e}"))?;

    // Call the handler function (may return a Promise for async handlers)
    let call_code = format!("{FN_NAME}(__request__)");
    let result = context
        .eval(Source::from_bytes(&call_code))
        .map_err(|e| format!("Failed to call {FN_NAME}: {e}"))?;

    // If the result is a Promise, drive it to completion
    let result = resolve_if_promise_async(&result, &mut context, &executor)
        .await
        .map_err(|e| format!("Promise resolution failed: {e}"))?;

    // Stringify the result
    json_stringify(&result, &mut context).map_err(|e| format!("Failed to stringify result: {e}"))
}

/// If `value` is a Promise, drive it to completion and return the resolved value (async version).
async fn resolve_if_promise_async(
    value: &JsValue,
    context: &mut Context,
    executor: &Rc<WasiJobExecutor>,
) -> JsResult<JsValue> {
    let Some(object) = value.as_object() else {
        return Ok(value.clone());
    };
    let Ok(promise) = JsPromise::from_object(object) else {
        return Ok(value.clone());
    };

    // Drive promise resolution using the executor's async job runner.
    let executor = executor.clone();
    let context = RefCell::new(context);
    loop {
        match promise.state() {
            PromiseState::Pending => {
                executor.clone().drive_jobs(&context).await?;
            }
            PromiseState::Fulfilled(v) => return Ok(v),
            PromiseState::Rejected(e) => return Err(JsError::from_opaque(e)),
        }
    }
}

/// Register the `fetch` API backed by WASIp2 HTTP.
fn setup_fetch(context: &mut Context) -> JsResult<()> {
    boa_runtime::register(FetchExtension(WasiFetcher), None, context)
}

/// Convert JSON response from JS to HTTP Response.
fn json_to_response(json: &str) -> Result<Response<Body>, wstd::http::Error> {
    #[derive(serde::Deserialize)]
    struct ResponseJson {
        status: u16,
        #[serde(default)]
        headers: Vec<(String, String)>,
        #[serde(default)]
        body: String,
    }

    match serde_json::from_str::<ResponseJson>(json) {
        Ok(r) => {
            let mut builder = Response::builder()
                .status(StatusCode::from_u16(r.status).unwrap_or(StatusCode::OK));
            for (k, v) in r.headers {
                builder = builder.header(k, v);
            }
            builder
                .body(Body::from(r.body))
                .map_err(wstd::http::Error::new)
        }
        Err(err) => {
            log::error(&format!("Failed to parse response JSON: {err}"));
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::from(format!("Invalid response format: {err}")))
                .map_err(wstd::http::Error::new)
        }
    }
}
