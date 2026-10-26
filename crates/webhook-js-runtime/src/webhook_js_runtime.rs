//! JavaScript webhook runtime using Boa JS engine.
//!
//! This is a WASI HTTP handler that runs JavaScript code to process HTTP requests.
//! The JS code is provided via the `JS_SOURCE` environment variable.
//!
//! # JS API Reference
//!
//! ## Basic Handler
//! ```js
//! export default function handle(request) {
//!     return new Response("Hello from JS!", {
//!         status: 200,
//!         headers: { "content-type": "text/plain" },
//!     });
//! }
//! ```
//!
//! ## JSON Response
//! ```js
//! export default function handle(request) {
//!     return Response.json({ ok: true });
//! }
//! ```
//!
//! ## Async Handler — Pass-Through Fetch
//! ```js
//! export default async function handle(request) {
//!     return fetch("https://api.example.com/data");
//! }
//! ```
//!
//! ## Streaming Response
//! Headers are sent when the handler returns; chunks are sent as they are enqueued.
//! ```js
//! export default function handle(request) {
//!     const stream = new ReadableStream({
//!         async pull(controller) {
//!             await new Promise(resolve => setTimeout(resolve, 1000));
//!             controller.enqueue("data: tick\n\n");
//!         },
//!     });
//!     return new Response(stream, { headers: { "content-type": "text/event-stream" } });
//! }
//! ```
//!
//! ## Proxying a Streamed Fetch Body
//! ```js
//! export default async function handle(request) {
//!     const upstream = await fetch("https://api.example.com/events");
//!     return new Response(upstream.body, { headers: { "content-type": "text/event-stream" } });
//! }
//! ```
//!
//! ## Reading the Request Body
//! ```js
//! // As plain text (returns Promise<string>)
//! const text = await request.text();
//!
//! // As parsed JSON (returns Promise<any>)
//! const data = await request.json();
//!
//! // As URL-encoded form data (returns Promise<object>)
//! const form = await request.formData();
//! const value = form["fieldName"];
//! ```
//!
//! ## Scheduling Executions
//! ```js
//! // Generate execution ID first
//! const execId = obelisk.executionIdGenerate();
//!
//! // Schedule immediately (scheduleAt is optional, defaults to now)
//! dynamic.schedule(execId, "ns:pkg/ifc.func", [arg1, arg2]);
//!
//! // Schedule with delay
//! dynamic.schedule(execId, "ns:pkg/ifc.func", [args], { seconds: 60 });
//! dynamic.schedule(execId, "ns:pkg/ifc.func", [args], { minutes: 5 });
//! dynamic.schedule(execId, "ns:pkg/ifc.func", [args], { hours: 1 });
//! ```
//!
//! ## Call and Wait for Result
//! ```js
//! // Synchronous call - blocks until completion
//! const result = dynamic.call("ns:pkg/ifc.func", [arg1, arg2]);
//! // Returns ok value, throws err value
//! ```
//!
//! ## Check Execution Status
//! ```js
//! const status = obelisk.getStatus(execId);
//! // status = "pendingAt" | "locked" | "paused" | "blockedByJoinSet" | "cancelling" | "finished"
//! // If finished: status.finishedStatus = "ok" | "err" | "executionFailure"
//! ```
//!
//! ## Get Execution Result
//! ```js
//! // Blocking - waits until execution completes
//! const result = obelisk.get(execId);
//! // Returns ok value, throws err value
//!
//! // Non-blocking - returns immediately
//! const result = obelisk.tryGet(execId);
//! // Returns ok value, throws err value, returns undefined if not finished yet.
//! ```
//!
//! ## Environment Variables
//! ```js
//! // Get environment variable value (returns string or undefined)
//! const value = process.env["MY_VAR"];
//! ```
//!
//! ## Get Current Execution ID
//! ```js
//! const execId = obelisk.executionIdCurrent();
//! ```
//!
//! ## Console Logging
//! ```js
//! console.log("info message");
//! console.debug("debug message");
//! console.warn("warning");
//! console.error("error");
//! ```

use crate::generated::obelisk::log::log;
use crate::generated::obelisk::types::backtrace::{FrameInfo, FrameSymbol, WasmBacktrace};
use crate::generated::obelisk::types::execution::{ExecutionFailureKind, ExecutionId};
use crate::generated::obelisk::types::function::Function;
use crate::generated::obelisk::types::time::{Datetime, Duration, ScheduleAt};
use crate::generated::obelisk::webhook::webhook_dynamic_support_backtrace;
use crate::generated::obelisk::webhook::webhook_support::{
    self, ExecutionStatus, ExecutionStatusFinished,
};
use crate::streaming;
use boa_common::child_error::{ChildErrorParts, make_child_error};
use boa_common::console::{ObeliskLogger, json_stringify, setup_console};
use boa_common::crypto::setup_crypto;
use boa_common::esm::{EsmError, get_default_export_from_module, resolve_promise};
use boa_common::graph::{GraphLoadError, register_source_modules};
use boa_common::helpers::{new_object, parse_ffqn};
use boa_common::imports::{self, ProxyKind};
use boa_common::wasi_job_executor::WasiJobExecutor;
use boa_engine::class::Class;
use boa_engine::module::MapModuleLoader;
use boa_engine::{
    Context, JsArgs, JsError, JsNativeError, JsResult, JsString, JsValue, NativeFunction, Source,
    js_string, object::builtins::JsDate,
};
use boa_runtime::fetch::request::JsRequest;
use boa_runtime::fetch::response::JsResponse;
use futures::channel::oneshot;
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::future::{Future, poll_fn};
use std::pin::{Pin, pin};
use std::rc::Rc;
use wstd::__internal::wasip2::http::types::{IncomingRequest, ResponseOutparam};
use wstd::http::body::Body;
use wstd::http::server::Responder;
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

struct Server;

// A hand-written export instead of `#[wstd::http_server]`: the JS runtime keeps pumping a
// streamed body while the responder sends it, and a client disconnect must not trap.
impl wstd::__internal::wasip2::exports::http::incoming_handler::Guest for Server {
    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        let responder = Responder::new(response_out);
        wstd::runtime::block_on(async move {
            let request = match wstd::http::request::try_from_incoming(request) {
                Ok(request) => request,
                Err(err) => return responder.fail(err),
            };
            let (response_tx, response_rx) = oneshot::channel();
            streaming::install_responder(response_tx);
            let mut handler = pin!(handle_request(request));
            let mut send = pin!(async move {
                match response_rx.await {
                    Ok(response) => responder.respond(response).await,
                    Err(_) => {
                        responder.fail(wstd::http::Error::msg("webhook ended without a response"));
                        Ok(())
                    }
                }
            });
            let mut handler_done = false;
            let sent = poll_fn(|cx| {
                if !handler_done {
                    handler_done = handler.as_mut().poll(cx).is_ready();
                }
                send.as_mut().poll(cx)
            })
            .await;
            match sent {
                // The body is complete; let the handler finish any work after closing it.
                Ok(()) if !handler_done => handler.await,
                Ok(()) => {}
                // The client is gone; abandon the handler.
                Err(err) => log::debug(&format!("response body was not fully sent: {err:?}")),
            }
        });
    }
}

wstd::__internal::wasip2::http::proxy::export!(Server with_types_in wstd::__internal::wasip2);

async fn handle_request(request: Request<Body>) {
    // Load JS module graph from environment.
    let (entry_path, files) = match read_js_graph_env() {
        Ok(graph) => graph,
        Err(msg) => {
            streaming::respond(error_response(msg));
            return;
        }
    };

    // Destructure the wstd request into HTTP parts and body.
    let (parts, mut incoming_body) = request.into_parts();

    // Build the request head (metadata only, no body yet).
    let mut builder = http::Request::builder()
        .method(parts.method.as_str())
        .uri(parts.uri.to_string().as_str());
    for (k, v) in &parts.headers {
        builder = builder.header(k, v);
    }
    let http_request_head = match builder.body(Vec::<u8>::new()) {
        Ok(r) => r,
        Err(e) => {
            streaming::respond(error_response(format!("Failed to construct request: {e}")));
            return;
        }
    };

    // Wrap the body as a lazy future: bytes are only read when JS calls text()/json()/formData().
    let body_future = Box::pin(async move {
        match incoming_body.contents().await {
            Ok(bytes) => bytes.to_vec(),
            Err(_) => Vec::new(),
        }
    });

    match run_js_handler_inner(&entry_path, &files, http_request_head, body_future).await {
        Ok(Some(response)) => {
            streaming::respond(response);
        }
        Ok(None) => {}
        Err(msg) => {
            log::error(&msg);
            // Once streaming started, only the body can carry the failure.
            if !streaming::respond(error_response(msg.clone())) {
                streaming::abort_body(msg).await;
            }
        }
    }
}

fn error_response(msg: String) -> Response<Body> {
    let mut response = Response::new(Body::from(msg));
    *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
    response
}

type RequestBodyFuture = Pin<Box<dyn Future<Output = Vec<u8>>>>;

/// Read the JS module graph from environment variables.
///
/// `__OBELISK_JS_FILES__` holds the JSON `{"<path>": "<source>", ...}` graph;
/// `__OBELISK_JS_ENTRY_PATH__` names the entry within that graph. Both must
/// be non-empty: the host guarantees a real path for every module.
fn read_js_graph_env() -> Result<(String, BTreeMap<String, String>), String> {
    let entry_path = std::env::var("__OBELISK_JS_ENTRY_PATH__")
        .map_err(|_| "__OBELISK_JS_ENTRY_PATH__ environment variable not set".to_string())?;
    let files_json = std::env::var("__OBELISK_JS_FILES__")
        .map_err(|_| "__OBELISK_JS_FILES__ environment variable not set".to_string())?;
    let files: BTreeMap<String, String> =
        serde_json::from_str::<Vec<(String, String)>>(&files_json)
            .map_err(|e| format!("Failed to parse __OBELISK_JS_FILES__: {e}"))?
            .into_iter()
            .collect();
    Ok((entry_path, files))
}

/// Create a `NativeFunction` that proxies a direct call to `call-json`
/// from `obelisk:webhook/webhook-support`.
fn create_direct_call_proxy(
    interface_name: &str,
    function_name: &str,
    context: &mut Context,
) -> JsValue {
    let ifc = js_string!(interface_name);
    let func = js_string!(function_name);

    let native = NativeFunction::from_copy_closure_with_captures(
        |_this, args, (ifc, func): &(JsString, JsString), ctx| {
            let function = Function {
                interface_name: ifc.to_std_string_escaped(),
                function_name: func.to_std_string_escaped(),
            };
            let array = boa_engine::object::builtins::JsArray::new(ctx)?;
            for (i, arg) in args.iter().enumerate() {
                array.set(i as u32, arg.clone(), false, ctx)?;
            }
            let params_json = json_stringify(&array.into(), ctx)?;

            let backtrace = capture_backtrace(ctx);
            match webhook_dynamic_support_backtrace::call_json(
                &function,
                &params_json,
                Some(&backtrace),
            ) {
                Ok(Ok(Some(json_str))) => ctx.eval(Source::from_bytes(&format!("({})", json_str))),
                Ok(Ok(None)) => Ok(JsValue::null()),
                Ok(Err(payload)) => {
                    let child = webhook_support::last_direct_call_id();
                    Err(call_child_error(
                        child.as_ref().map(|e| e.id.as_str()),
                        payload,
                        ctx,
                    )?)
                }
                Err(e) => Err(JsNativeError::error()
                    .with_message(format!("call failed: {:?}", e))
                    .into()),
            }
        },
        (ifc, func),
    );
    native.to_js_function(context.realm()).into()
}

/// Create a `NativeFunction` that proxies a schedule call via
/// `execution-id-generate` + `schedule-json` from `obelisk:webhook/webhook-support`.
///
/// JS signature: `myFuncSchedule(scheduleAt, ...args) → executionId`
fn create_schedule_proxy(
    interface_name: &str,
    function_name: &str,
    with_id: bool,
    context: &mut Context,
) -> JsValue {
    let ifc = js_string!(interface_name);
    let func = js_string!(function_name);

    let native = NativeFunction::from_copy_closure_with_captures(
        |_this, args, (ifc, func, with_id): &(JsString, JsString, bool), ctx| {
            let function = Function {
                interface_name: ifc.to_std_string_escaped(),
                function_name: func.to_std_string_escaped(),
            };

            let offset = usize::from(*with_id);
            let schedule_value = args.get_or_undefined(offset);
            let schedule = parse_schedule_at(schedule_value, ctx)?;

            let array = boa_engine::object::builtins::JsArray::new(ctx)?;
            for (i, arg) in args.iter().skip(offset + 1).enumerate() {
                array.set(i as u32, arg.clone(), false, ctx)?;
            }
            let params_json = json_stringify(&array.into(), ctx)?;

            let backtrace = capture_backtrace(ctx);
            let explicit_id = if *with_id {
                Some(args.get_or_undefined(0).clone())
            } else {
                schedule_value
                    .as_object()
                    .map(|obj| obj.get(js_string!("executionId"), ctx))
                    .transpose()?
            };
            let exec_id = match explicit_id.filter(|id| *with_id || !id.is_undefined()) {
                Some(id) => ExecutionId {
                    id: id
                        .as_string()
                        .ok_or_else(|| {
                            JsNativeError::typ().with_message("executionId must be a string")
                        })?
                        .to_std_string_escaped(),
                },
                None => webhook_support::execution_id_generate(),
            };

            match webhook_dynamic_support_backtrace::schedule_json(
                &exec_id,
                schedule,
                &function,
                &params_json,
                Some(&backtrace),
            ) {
                Ok(()) => Ok(JsValue::from(js_string!(exec_id.id))),
                Err(e) => Err(JsNativeError::error()
                    .with_message(format!("schedule failed: {:?}", e))
                    .into()),
            }
        },
        (ifc, func, with_id),
    );
    native.to_js_function(context.realm()).into()
}

/// Webhook proxy factory for [`imports::register_import_modules`].
///
/// Webhooks support direct calls and schedule only — ext imports are not supported.
fn create_webhook_proxy(kind: ProxyKind, context: &mut Context) -> JsValue {
    match kind {
        ProxyKind::DirectCall {
            interface_name,
            function_name,
        } => create_direct_call_proxy(interface_name, function_name, context),
        ProxyKind::Schedule {
            interface_name,
            function_name,
            with_id,
        } => create_schedule_proxy(interface_name, function_name, with_id, context),
        ProxyKind::ExtSubmit { .. }
        | ProxyKind::ExtAwaitNext { .. }
        | ProxyKind::ExtGet
        | ProxyKind::Stub { .. } => {
            unreachable!("webhooks do not support -obelisk-ext or -obelisk-stub imports")
        }
    }
}

/// Read resolved imports from the `__OBELISK_RESOLVED_IMPORTS__` env var.
fn read_resolved_imports() -> HashMap<String, Vec<(String, String)>> {
    match std::env::var("__OBELISK_RESOLVED_IMPORTS__") {
        Ok(json) => serde_json::from_str(&json).unwrap_or_default(),
        Err(_) => HashMap::new(),
    }
}

async fn run_js_handler_inner(
    entry_path: &str,
    files: &BTreeMap<String, String>,
    request_head: http::Request<Vec<u8>>,
    body_future: RequestBodyFuture,
) -> Result<Option<Response<Body>>, String> {
    let executor = Rc::new(WasiJobExecutor::default());
    let loader = Rc::new(MapModuleLoader::new());
    let mut context = Context::builder()
        .job_executor(executor.clone())
        .module_loader(loader.clone())
        .build()
        .expect("building context must work");

    // Set up console.log -> obelisk:log
    setup_console(&mut context, Logger).expect("console setup must work");

    // Set up fetch, Request, Response and ReadableStream, plus the response streaming shim
    let serve = streaming::setup(&mut context).expect("streaming setup must work");

    // Set up crypto.subtle (HMAC-SHA-256/384/512)
    setup_crypto(&mut context).expect("crypto setup must work");

    // Set up the obelisk global object with webhook support APIs
    let dynamic_enabled = imports::declared_modules(files.values().map(String::as_str))?
        .contains("obelisk:webhook-dynamic@1.0.0");
    setup_obelisk_api(&loader, &mut context, dynamic_enabled).expect("obelisk API setup must work");

    // Register synthetic modules for WIT-style imports (e.g., 'ns:pkg/ifc').
    let resolved_imports = read_resolved_imports();
    if !resolved_imports.is_empty() {
        imports::register_import_modules(
            &resolved_imports,
            &loader,
            &mut context,
            &create_webhook_proxy,
        );
    }

    // Parse every file in the graph and register with the loader.
    let entry_module = match register_source_modules(&loader, files, entry_path, &mut context) {
        Ok(module) => module,
        Err(GraphLoadError::EntryNotFound) => {
            return Err(format!("Entry path `{entry_path}` not found in files"));
        }
        Err(GraphLoadError::ParseError { path, message }) => {
            return Err(format!("Module parse error in `{path}`: {message}"));
        }
    };

    // Wrap the incoming request as a JS Request object so the handler receives a proper
    // Request object with text(), json(), formData(), method, url, and headers.
    let js_request = JsRequest::with_lazy_body(request_head, body_future, &mut context);
    let request_obj = JsRequest::from_data(js_request, &mut context)
        .map_err(|e| format!("Failed to create JS Request object: {e}"))?;
    let request_value = JsValue::from(request_obj);

    // We need to wrap context in RefCell for the async ESM loading
    let context = RefCell::new(&mut context);

    // Get the default export function from the entry module
    let default_fn = match get_default_export_from_module(entry_module, &context, &executor).await {
        Ok(func) => func,
        Err(EsmError::LoadError(msg)) => return Err(format!("Module load error: {msg}")),
        Err(EsmError::LinkError(msg)) => return Err(format!("Module link error: {msg}")),
        Err(EsmError::EvalError(msg)) => return Err(format!("Module eval error: {msg}")),
        Err(EsmError::NoDefaultExport) => return Err("No default export found".to_string()),
        Err(EsmError::DefaultNotCallable) => {
            return Err("Default export is not callable".to_string());
        }
    };

    // Run the handler through the streaming shim, which pumps a streamed body itself.
    let result = serve
        .call(
            &JsValue::undefined(),
            &[default_fn.into(), request_value],
            *context.borrow_mut(),
        )
        .map_err(|e| format!("Failed to call handler: {e}"))?;
    let result = resolve_promise(&result, &context, &executor)
        .await
        .map_err(|e| format!("Promise resolution failed: {e}"))?;
    let Some(obj) = result.as_object() else {
        return Ok(None);
    };
    let js_response = obj.downcast_ref::<JsResponse>().ok_or(
        "handler must return a Response (e.g. `new Response(...)` or `Response.json(...)`)",
    )?;

    let (mut parts, body) =
        Response::new(Body::from(js_response.body().as_ref().clone())).into_parts();
    parts.status = StatusCode::from_u16(js_response.status()).unwrap_or(StatusCode::OK);
    parts.headers = js_response.headers().as_header_map().borrow().clone();
    Ok(Some(Response::from_parts(parts, body)))
}

/// Capture the current Boa JS stack trace when enabled by the webhook configuration.
fn capture_backtrace(ctx: &Context) -> WasmBacktrace {
    if std::env::var_os("__OBELISK_BACKTRACE_ENABLED__").is_none() {
        return WasmBacktrace { frames: Vec::new() };
    }
    use boa_engine::vm::SourcePath;
    let js_file_name = std::env::var("__OBELISK_JS_ENTRY_PATH__").ok();
    let frames = ctx
        .stack_trace()
        .map(|frame| {
            let loc = frame.position();
            let module = match &loc.path {
                SourcePath::Path(p) => p.display().to_string(),
                _ => js_file_name
                    .clone()
                    .unwrap_or_else(|| "unknown".to_string()),
            };
            let file = match &loc.path {
                SourcePath::Path(p) => Some(p.display().to_string()),
                _ => js_file_name.clone(),
            };
            let symbol = FrameSymbol {
                func_name: None,
                file,
                line: loc.position.map(|p| p.line_number()),
                col: loc.position.map(|p| p.column_number()),
            };
            FrameInfo {
                module,
                func_name: loc.function_name.to_std_string_escaped(),
                symbols: vec![symbol],
            }
        })
        .collect();
    WasmBacktrace { frames }
}

/// Unwrap a `Result<Option<String>, Option<String>>` from a child outcome into a
/// JS value: returns the ok value, or throws an [`obelisk.ChildError`]
/// built from the given child `exec_id`.
fn unwrap_result(
    inner_result: Result<Option<String>, Option<String>>,
    exec_id: &str,
    ctx: &mut Context,
) -> JsResult<JsValue> {
    match inner_result {
        Ok(Some(json_str)) => ctx.eval(Source::from_bytes(&format!("({})", json_str))),
        Ok(None) => Ok(JsValue::null()),
        Err(payload) => Err(child_error(exec_id, payload, ctx)?),
    }
}

/// Kebab-case string for a WIT `execution-failure-kind`, used as
/// `ChildError.failureKind`.
fn exec_failure_kind_str(kind: ExecutionFailureKind) -> &'static str {
    match kind {
        ExecutionFailureKind::TimedOut => "timed-out",
        ExecutionFailureKind::NondeterminismDetected => "nondeterminism-detected",
        ExecutionFailureKind::OutOfFuel => "out-of-fuel",
        ExecutionFailureKind::Cancelled => "cancelled",
        ExecutionFailureKind::ValueTooLarge => "value-too-large",
        ExecutionFailureKind::Uncategorized => "uncategorized",
    }
}

/// Build a `ChildError` for a failed child execution, disambiguating a
/// business `err` from a platform failure via `get-execution-failure-kind`.
fn child_error(exec_id: &str, payload: Option<String>, ctx: &mut Context) -> JsResult<JsError> {
    let exec = ExecutionId {
        id: exec_id.to_string(),
    };
    match webhook_support::get_execution_failure_kind(&exec) {
        Ok(Some(kind)) => {
            let cancelled = matches!(kind, ExecutionFailureKind::Cancelled);
            make_child_error(
                &ChildErrorParts {
                    child_id: Some(exec_id),
                    delay_id: None,
                    value_json: payload.as_deref(),
                    failure_kind: Some(exec_failure_kind_str(kind)),
                    cancelled,
                    message: None,
                },
                ctx,
            )
        }
        Ok(None) => make_child_error(
            &ChildErrorParts {
                child_id: Some(exec_id),
                delay_id: None,
                value_json: payload.as_deref(),
                failure_kind: None,
                cancelled: false,
                message: None,
            },
            ctx,
        ),
        Err(e) => Ok(JsNativeError::error()
            .with_message(format!("failed to resolve child failure kind: {:?}", e))
            .into()),
    }
}

/// Build a `ChildError` for a failed direct call (`dynamic.call` / an
/// import proxy), whose child id comes from `last-direct-call-id`.
fn call_child_error(
    child_id: Option<&str>,
    payload: Option<String>,
    ctx: &mut Context,
) -> JsResult<JsError> {
    match child_id {
        Some(id) => child_error(id, payload, ctx),
        None => make_child_error(
            &ChildErrorParts {
                child_id: None,
                delay_id: None,
                value_json: payload.as_deref(),
                failure_kind: None,
                cancelled: false,
                message: None,
            },
            ctx,
        ),
    }
}

/// Set up the global `obelisk` object with webhook support functions.
fn setup_obelisk_api(
    loader: &MapModuleLoader,
    context: &mut Context,
    dynamic_enabled: bool,
) -> JsResult<()> {
    let obelisk = new_object(context);

    // obelisk.executionIdGenerate()
    let generate_execution_id_fn = NativeFunction::from_fn_ptr(|_this, _args, _ctx| {
        let exec_id = webhook_support::execution_id_generate();
        Ok(JsValue::from(js_string!(exec_id.id)))
    });
    obelisk.set(
        js_string!("executionIdGenerate"),
        generate_execution_id_fn.to_js_function(context.realm()),
        false,
        context,
    )?;

    // obelisk.executionIdCurrent()
    let current_execution_id_fn = NativeFunction::from_fn_ptr(|_this, _args, _ctx| {
        let exec_id = webhook_support::execution_id_current();
        Ok(JsValue::from(js_string!(exec_id.id)))
    });
    obelisk.set(
        js_string!("executionIdCurrent"),
        current_execution_id_fn.to_js_function(context.realm()),
        false,
        context,
    )?;

    // dynamic.schedule(executionId, ffqn, params, scheduleAt?)
    let schedule_fn = NativeFunction::from_fn_ptr(|_this, args, ctx| {
        let exec_id_str = args
            .get_or_undefined(0)
            .as_string()
            .ok_or_else(|| JsNativeError::typ().with_message("executionId must be a string"))?
            .to_std_string_escaped();
        let exec_id = ExecutionId { id: exec_id_str };

        let ffqn = args
            .get_or_undefined(1)
            .as_string()
            .ok_or_else(|| JsNativeError::typ().with_message("ffqn must be a string"))?
            .to_std_string_escaped();

        let (ifc_name, fn_name) = parse_ffqn(&ffqn)?;
        let function = Function {
            interface_name: ifc_name,
            function_name: fn_name,
        };

        let params_val = args.get_or_undefined(2);
        let params_json = json_stringify(params_val, ctx)?;

        let schedule = if let Some(schedule_val) = args.get(3) {
            parse_schedule_at(schedule_val, ctx)?
        } else {
            ScheduleAt::Now
        };

        let backtrace = capture_backtrace(ctx);
        match webhook_dynamic_support_backtrace::schedule_json(
            &exec_id,
            schedule,
            &function,
            &params_json,
            Some(&backtrace),
        ) {
            Ok(()) => Ok(JsValue::undefined()),
            Err(e) => Err(JsNativeError::error()
                .with_message(format!("schedule failed: {:?}", e))
                .into()),
        }
    });
    obelisk.set(
        js_string!("schedule"),
        schedule_fn.to_js_function(context.realm()),
        false,
        context,
    )?;

    // obelisk.getStatus(executionId)
    let get_status_fn = NativeFunction::from_fn_ptr(|_this, args, ctx| {
        let exec_id_str = args
            .get_or_undefined(0)
            .as_string()
            .ok_or_else(|| JsNativeError::typ().with_message("executionId must be a string"))?
            .to_std_string_escaped();

        let exec_id = ExecutionId { id: exec_id_str };

        match webhook_support::get_status(&exec_id) {
            Ok(status) => {
                let result_obj = new_object(ctx);
                match status {
                    ExecutionStatus::PendingAt(dt) => {
                        result_obj.set(
                            js_string!("status"),
                            js_string!("pendingAt"),
                            false,
                            ctx,
                        )?;
                        let dt_obj = new_object(ctx);
                        dt_obj.set(js_string!("seconds"), JsValue::from(dt.seconds), false, ctx)?;
                        dt_obj.set(
                            js_string!("nanoseconds"),
                            JsValue::from(dt.nanoseconds),
                            false,
                            ctx,
                        )?;
                        result_obj.set(js_string!("pendingAt"), dt_obj, false, ctx)?;
                    }
                    ExecutionStatus::Locked => {
                        result_obj.set(js_string!("status"), js_string!("locked"), false, ctx)?;
                    }
                    ExecutionStatus::Paused => {
                        result_obj.set(js_string!("status"), js_string!("paused"), false, ctx)?;
                    }
                    ExecutionStatus::BlockedByJoinSet => {
                        result_obj.set(
                            js_string!("status"),
                            js_string!("blockedByJoinSet"),
                            false,
                            ctx,
                        )?;
                    }
                    ExecutionStatus::Cancelling => {
                        result_obj.set(
                            js_string!("status"),
                            js_string!("cancelling"),
                            false,
                            ctx,
                        )?;
                    }
                    ExecutionStatus::Finished(finished) => {
                        result_obj.set(js_string!("status"), js_string!("finished"), false, ctx)?;
                        let finished_status = match finished {
                            ExecutionStatusFinished::Ok => "ok",
                            ExecutionStatusFinished::Err => "err",
                            ExecutionStatusFinished::ExecutionFailure(_) => "executionFailure",
                        };
                        result_obj.set(
                            js_string!("finishedStatus"),
                            js_string!(finished_status),
                            false,
                            ctx,
                        )?;
                    }
                }
                Ok(result_obj.into())
            }
            Err(e) => Err(JsNativeError::error()
                .with_message(format!("getStatus failed: {:?}", e))
                .into()),
        }
    });
    obelisk.set(
        js_string!("getStatus"),
        get_status_fn.to_js_function(context.realm()),
        false,
        context,
    )?;

    // obelisk.get(executionId) - blocking get, returns ok value, throws err value
    let get_fn = NativeFunction::from_fn_ptr(|_this, args, ctx| {
        let exec_id_str = args
            .get_or_undefined(0)
            .as_string()
            .ok_or_else(|| JsNativeError::typ().with_message("executionId must be a string"))?
            .to_std_string_escaped();

        let exec_id = ExecutionId {
            id: exec_id_str.clone(),
        };

        match webhook_support::get(&exec_id) {
            Ok(inner_result) => unwrap_result(inner_result, &exec_id_str, ctx),
            Err(e) => Err(JsNativeError::error()
                .with_message(format!("get failed: {:?}", e))
                .into()),
        }
    });
    obelisk.set(
        js_string!("get"),
        get_fn.to_js_function(context.realm()),
        false,
        context,
    )?;

    // obelisk.tryGet(executionId) - non-blocking get
    // Returns ok value, throws err value, returns undefined if not finished yet
    let try_get_fn = NativeFunction::from_fn_ptr(|_this, args, ctx| {
        let exec_id_str = args
            .get_or_undefined(0)
            .as_string()
            .ok_or_else(|| JsNativeError::typ().with_message("executionId must be a string"))?
            .to_std_string_escaped();

        let exec_id = ExecutionId {
            id: exec_id_str.clone(),
        };

        match webhook_support::try_get(&exec_id) {
            Ok(inner_result) => unwrap_result(inner_result, &exec_id_str, ctx),
            Err(webhook_support::TryGetError::NotFinishedYet) => Ok(JsValue::undefined()),
            Err(e) => Err(JsNativeError::error()
                .with_message(format!("tryGet failed: {:?}", e))
                .into()),
        }
    });
    obelisk.set(
        js_string!("tryGet"),
        try_get_fn.to_js_function(context.realm()),
        false,
        context,
    )?;

    // dynamic.call(ffqn, params) - call child execution and wait for result
    let call_fn = NativeFunction::from_fn_ptr(|_this, args, ctx| {
        let backtrace = capture_backtrace(ctx);

        let ffqn = args
            .get_or_undefined(0)
            .as_string()
            .ok_or_else(|| JsNativeError::typ().with_message("ffqn must be a string"))?
            .to_std_string_escaped();

        let (ifc_name, fn_name) = parse_ffqn(&ffqn)?;
        let function = Function {
            interface_name: ifc_name,
            function_name: fn_name,
        };

        let params_val = args.get_or_undefined(1);
        let params_json = json_stringify(params_val, ctx)?;

        // Call child execution and wait for result
        match webhook_dynamic_support_backtrace::call_json(
            &function,
            &params_json,
            Some(&backtrace),
        ) {
            Ok(Ok(Some(json_str))) => {
                let parsed = ctx.eval(Source::from_bytes(&format!("({})", json_str)))?;
                Ok(parsed)
            }
            Ok(Ok(None)) => Ok(JsValue::null()),
            Ok(Err(payload)) => {
                let child = webhook_support::last_direct_call_id();
                Err(call_child_error(
                    child.as_ref().map(|e| e.id.as_str()),
                    payload,
                    ctx,
                )?)
            }
            Err(e) => Err(JsNativeError::error()
                .with_message(format!("call failed: {:?}", e))
                .into()),
        }
    });
    obelisk.set(
        js_string!("call"),
        call_fn.to_js_function(context.realm()),
        false,
        context,
    )?;

    boa_common::child_error::register(&obelisk, context)?;
    let dynamic = new_object(context);
    dynamic.set(
        js_string!("call"),
        obelisk.get(js_string!("call"), context)?,
        false,
        context,
    )?;
    dynamic.set(
        js_string!("schedule"),
        obelisk.get(js_string!("schedule"), context)?,
        false,
        context,
    )?;
    imports::register_builtin_module(
        "obelisk:webhook@1.0.0",
        &[
            "executionIdGenerate",
            "executionIdCurrent",
            "getStatus",
            "get",
            "tryGet",
            "ChildError",
        ],
        &obelisk,
        loader,
        context,
    );
    if dynamic_enabled {
        imports::register_builtin_module(
            "obelisk:webhook-dynamic@1.0.0",
            &["call", "schedule"],
            &dynamic,
            loader,
            context,
        );
    }

    Ok(())
}

/// Parse a schedule specification from JS value.
fn parse_schedule_at(value: &JsValue, ctx: &mut Context) -> JsResult<ScheduleAt> {
    if value.is_undefined() || value.is_null() {
        return Ok(ScheduleAt::Now);
    }

    let obj = value
        .as_object()
        .ok_or_else(|| JsNativeError::typ().with_message("schedule must be an object"))?;

    // A JS `Date` is accepted as an absolute wake-up time; `sleep` also returns a
    // `Date`, making the API symmetric.
    if let Ok(date) = JsDate::from_object(obj.clone()) {
        let millis = date.get_time(ctx)?.to_number(ctx)?;
        if millis.is_nan() {
            return Err(JsNativeError::typ()
                .with_message("schedule Date is an Invalid Date")
                .into());
        }
        let millis = millis as i64;
        let seconds = (millis / 1000).max(0) as u64;
        let nanoseconds = ((millis % 1000).max(0) * 1_000_000) as u32;
        return Ok(ScheduleAt::At(Datetime {
            seconds,
            nanoseconds,
        }));
    }

    // The duration keys and `at` are mutually exclusive: they are not summed, so
    // more than one is a mistake. Reject it instead of silently picking one by
    // precedence order.
    let present: Vec<&str> = ["milliseconds", "seconds", "minutes", "hours", "days", "at"]
        .into_iter()
        .filter(|key| {
            obj.get(JsString::from(*key), ctx)
                .is_ok_and(|v| !v.is_undefined())
        })
        .collect();
    if present.len() > 1 {
        return Err(JsNativeError::typ()
            .with_message(format!(
                "schedule object has multiple keys ({}); specify exactly one of milliseconds, seconds, minutes, hours, days, or at",
                present.join(", ")
            ))
            .into());
    }

    // Check for different duration types
    if let Ok(ms) = obj.get(js_string!("milliseconds"), ctx)
        && !ms.is_undefined()
    {
        let ms_val = ms.to_u32(ctx)? as u64;
        return Ok(ScheduleAt::In(Duration::Milliseconds(ms_val)));
    }

    if let Ok(secs) = obj.get(js_string!("seconds"), ctx)
        && !secs.is_undefined()
    {
        let secs_val = secs.to_u32(ctx)? as u64;
        return Ok(ScheduleAt::In(Duration::Seconds(secs_val)));
    }

    if let Ok(mins) = obj.get(js_string!("minutes"), ctx)
        && !mins.is_undefined()
    {
        let mins_val = mins.to_u32(ctx)?;
        return Ok(ScheduleAt::In(Duration::Minutes(mins_val)));
    }

    if let Ok(hours) = obj.get(js_string!("hours"), ctx)
        && !hours.is_undefined()
    {
        let hours_val = hours.to_u32(ctx)?;
        return Ok(ScheduleAt::In(Duration::Hours(hours_val)));
    }

    if let Ok(days) = obj.get(js_string!("days"), ctx)
        && !days.is_undefined()
    {
        let days_val = days.to_u32(ctx)?;
        return Ok(ScheduleAt::In(Duration::Days(days_val)));
    }

    // Check for absolute time
    if let Ok(at) = obj.get(js_string!("at"), ctx)
        && !at.is_undefined()
    {
        let at_obj = at.as_object().ok_or_else(|| {
            JsNativeError::typ().with_message("'at' must be an object with seconds and nanoseconds")
        })?;
        let seconds = at_obj.get(js_string!("seconds"), ctx)?.to_u32(ctx)? as u64;
        let nanoseconds = at_obj.get(js_string!("nanoseconds"), ctx)?.to_u32(ctx)?;
        return Ok(ScheduleAt::At(Datetime {
            seconds,
            nanoseconds,
        }));
    }

    if !obj.get(js_string!("executionId"), ctx)?.is_undefined() {
        return Ok(ScheduleAt::Now);
    }
    Err(JsNativeError::typ()
        .with_message(
            "schedule object has no recognized key (milliseconds, seconds, minutes, hours, days, at) and is not a Date",
        )
        .into())
}
