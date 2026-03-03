//! JavaScript webhook runtime using Boa JS engine.
//!
//! This is a WASI HTTP handler that runs JavaScript code to process HTTP requests.
//! The JS code is provided via the `JS_SOURCE` environment variable.
//!
//! # JS API Reference
//!
//! ## Basic Handler
//! ```js
//! export default function(request) {
//!     // request = { method, uri, headers, body }
//!     return {
//!         status: 200,
//!         headers: [["content-type", "text/plain"]],
//!         body: "Hello from JS!"
//!     };
//! }
//! ```
//!
//! ## Async Handlers with Fetch
//! ```js
//! export default async function(request) {
//!     const resp = await fetch("https://example.com/api");
//!     const data = await resp.text();
//!     return { status: 200, headers: [], body: data };
//! }
//! ```
//!
//! ## Scheduling Executions
//! ```js
//! // Generate execution ID first
//! const execId = obelisk.generateExecutionId();
//!
//! // Schedule immediately (scheduleAt is optional, defaults to now)
//! obelisk.schedule(execId, "ns:pkg/ifc.func", [arg1, arg2]);
//!
//! // Schedule with delay
//! obelisk.schedule(execId, "ns:pkg/ifc.func", [args], { seconds: 60 });
//! obelisk.schedule(execId, "ns:pkg/ifc.func", [args], { minutes: 5 });
//! obelisk.schedule(execId, "ns:pkg/ifc.func", [args], { hours: 1 });
//! ```
//!
//! ## Call and Wait for Result
//! ```js
//! // Synchronous call - blocks until completion
//! const result = obelisk.call("ns:pkg/ifc.func", [arg1, arg2]);
//! // result = { ok: value } or { err: value }
//! ```
//!
//! ## Check Execution Status
//! ```js
//! const status = obelisk.getStatus(execId);
//! // status.type = "pending" | "locked" | "blockedByJoinSet" | "finished"
//! // If finished: status.finished = "ok" | "err" | "executionFailure"
//! ```
//!
//! ## Get Execution Result
//! ```js
//! // Blocking - waits until execution completes
//! const result = obelisk.get(execId);
//! // result = { ok: value } or { err: value }
//!
//! // Non-blocking - returns immediately or throws if not finished
//! try {
//!     const result = obelisk.tryGet(execId);
//! } catch (e) {
//!     // "not finished yet" or "not found"
//! }
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
use crate::generated::obelisk::types::execution::{ExecutionId, Function, SubmitConfig};
use crate::generated::obelisk::types::time::{Datetime, Duration, ScheduleAt};
use crate::generated::obelisk::webhook::webhook_support::{
    self, ExecutionStatus, ExecutionStatusFinished,
};
use boa_common::console::{ObeliskLogger, json_stringify, setup_console};
use boa_common::esm::{EsmError, get_default_export, resolve_promise};
use boa_common::helpers::{new_object, parse_ffqn};
use boa_common::wasi_fetcher::WasiFetcher;
use boa_common::wasi_job_executor::WasiJobExecutor;
use boa_engine::{
    Context, JsArgs, JsNativeError, JsResult, JsValue, NativeFunction, Source, js_string,
    property::Attribute,
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

    // Set up the obelisk global object with webhook support APIs
    setup_obelisk_api(&mut context).expect("obelisk API setup must work");

    // We need to wrap context in RefCell for the async ESM loading
    let context = RefCell::new(&mut context);

    // Get the default export function from the ES module
    let default_fn = match get_default_export(js_source, &context, &executor).await {
        Ok(func) => func,
        Err(EsmError::ParseError(msg)) => return Err(format!("Module parse error: {msg}")),
        Err(EsmError::LoadError(msg)) => return Err(format!("Module load error: {msg}")),
        Err(EsmError::LinkError(msg)) => return Err(format!("Module link error: {msg}")),
        Err(EsmError::EvalError(msg)) => return Err(format!("Module eval error: {msg}")),
        Err(EsmError::NoDefaultExport) => return Err("No default export found".to_string()),
        Err(EsmError::DefaultNotCallable) => {
            return Err("Default export is not callable".to_string());
        }
    };

    // Parse the request JSON as a JS value
    let request_value = context
        .borrow_mut()
        .eval(Source::from_bytes(&format!("({request_json})")))
        .map_err(|e| format!("Failed to parse request: {e}"))?;

    // Call the default export function with the request (may return a Promise for async handlers)
    let result = default_fn
        .call(
            &JsValue::undefined(),
            &[request_value],
            *context.borrow_mut(),
        )
        .map_err(|e| format!("Failed to call handler: {e}"))?;

    // If the result is a Promise, drive it to completion
    let result = resolve_promise(&result, &context, &executor)
        .await
        .map_err(|e| format!("Promise resolution failed: {e}"))?;

    // Stringify the result
    json_stringify(&result, *context.borrow_mut())
        .map_err(|e| format!("Failed to stringify result: {e}"))
}

/// Register the `fetch` API backed by WASIp2 HTTP.
fn setup_fetch(context: &mut Context) -> JsResult<()> {
    boa_runtime::register(FetchExtension(WasiFetcher), None, context)
}

/// Set up the global `obelisk` object with webhook support functions.
fn setup_obelisk_api(context: &mut Context) -> JsResult<()> {
    let obelisk = new_object(context);

    // obelisk.generateExecutionId()
    let generate_execution_id_fn = NativeFunction::from_fn_ptr(|_this, _args, _ctx| {
        let exec_id = webhook_support::execution_id_generate();
        Ok(JsValue::from(js_string!(exec_id.id)))
    });
    obelisk.set(
        js_string!("generateExecutionId"),
        generate_execution_id_fn.to_js_function(context.realm()),
        false,
        context,
    )?;

    // obelisk.schedule(executionId, ffqn, params, scheduleAt?, config?)
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

        let config = if let Some(cfg_val) = args.get(4) {
            parse_submit_config(cfg_val, ctx)?
        } else {
            None
        };

        match webhook_support::schedule_json(&exec_id, schedule, &function, &params_json, config) {
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
                    ExecutionStatus::Finished(finished) => {
                        result_obj.set(js_string!("status"), js_string!("finished"), false, ctx)?;
                        let finished_status = match finished {
                            ExecutionStatusFinished::Ok => "ok",
                            ExecutionStatusFinished::Err => "err",
                            ExecutionStatusFinished::ExecutionFailure => "executionFailure",
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

    // obelisk.get(executionId) - blocking get
    let get_fn = NativeFunction::from_fn_ptr(|_this, args, ctx| {
        let exec_id_str = args
            .get_or_undefined(0)
            .as_string()
            .ok_or_else(|| JsNativeError::typ().with_message("executionId must be a string"))?
            .to_std_string_escaped();

        let exec_id = ExecutionId { id: exec_id_str };

        match webhook_support::get(&exec_id) {
            Ok(inner_result) => {
                let result_obj = new_object(ctx);
                match inner_result {
                    Ok(Some(json_str)) => {
                        let parsed = ctx.eval(Source::from_bytes(&format!("({})", json_str)))?;
                        result_obj.set(js_string!("ok"), parsed, false, ctx)?;
                    }
                    Ok(None) => {
                        result_obj.set(js_string!("ok"), JsValue::null(), false, ctx)?;
                    }
                    Err(Some(err_str)) => {
                        let parsed = ctx.eval(Source::from_bytes(&format!("({})", err_str)))?;
                        result_obj.set(js_string!("err"), parsed, false, ctx)?;
                    }
                    Err(None) => {
                        result_obj.set(js_string!("err"), JsValue::null(), false, ctx)?;
                    }
                }
                Ok(result_obj.into())
            }
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
    let try_get_fn = NativeFunction::from_fn_ptr(|_this, args, ctx| {
        let exec_id_str = args
            .get_or_undefined(0)
            .as_string()
            .ok_or_else(|| JsNativeError::typ().with_message("executionId must be a string"))?
            .to_std_string_escaped();

        let exec_id = ExecutionId { id: exec_id_str };

        match webhook_support::try_get(&exec_id) {
            Ok(inner_result) => {
                let result_obj = new_object(ctx);
                match inner_result {
                    Ok(Some(json_str)) => {
                        let parsed = ctx.eval(Source::from_bytes(&format!("({})", json_str)))?;
                        result_obj.set(js_string!("ok"), parsed, false, ctx)?;
                    }
                    Ok(None) => {
                        result_obj.set(js_string!("ok"), JsValue::null(), false, ctx)?;
                    }
                    Err(Some(err_str)) => {
                        let parsed = ctx.eval(Source::from_bytes(&format!("({})", err_str)))?;
                        result_obj.set(js_string!("err"), parsed, false, ctx)?;
                    }
                    Err(None) => {
                        result_obj.set(js_string!("err"), JsValue::null(), false, ctx)?;
                    }
                }
                Ok(result_obj.into())
            }
            Err(webhook_support::TryGetError::NotFinishedYet) => {
                let result_obj = new_object(ctx);
                result_obj.set(js_string!("pending"), JsValue::from(true), false, ctx)?;
                Ok(result_obj.into())
            }
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

    // obelisk.call(ffqn, params, config?) - call child execution and wait for result
    let call_fn = NativeFunction::from_fn_ptr(|_this, args, ctx| {
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

        let config = if let Some(cfg_val) = args.get(2) {
            parse_submit_config(cfg_val, ctx)?
        } else {
            None
        };

        // Call child execution and wait for result
        match webhook_support::call_json(&function, &params_json, config) {
            Ok(Ok(Some(json_str))) => {
                let parsed = ctx.eval(Source::from_bytes(&format!("({})", json_str)))?;
                Ok(parsed)
            }
            Ok(Ok(None)) => Ok(JsValue::null()),
            Ok(Err(Some(err_str))) => Err(JsNativeError::error().with_message(err_str).into()),
            Ok(Err(None)) => Err(JsNativeError::error()
                .with_message("child execution failed")
                .into()),
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

    // obelisk.env(key) - get environment variable
    let env_fn = NativeFunction::from_fn_ptr(|_this, args, _ctx| {
        let key = args
            .get_or_undefined(0)
            .as_string()
            .ok_or_else(|| JsNativeError::typ().with_message("key must be a string"))?
            .to_std_string_escaped();

        match std::env::var(&key) {
            Ok(value) => Ok(JsValue::from(js_string!(value))),
            Err(_) => Ok(JsValue::undefined()),
        }
    });
    obelisk.set(
        js_string!("env"),
        env_fn.to_js_function(context.realm()),
        false,
        context,
    )?;

    // Set obelisk as global
    context.register_global_property(js_string!("obelisk"), obelisk, Attribute::all())?;

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

    Ok(ScheduleAt::Now)
}

/// Parse submit config from JS value.
fn parse_submit_config(value: &JsValue, ctx: &mut Context) -> JsResult<Option<SubmitConfig>> {
    if value.is_undefined() || value.is_null() {
        return Ok(None);
    }

    let obj = value
        .as_object()
        .ok_or_else(|| JsNativeError::typ().with_message("config must be an object"))?;

    let timeout = if let Ok(timeout_val) = obj.get(js_string!("timeout"), ctx) {
        if !timeout_val.is_undefined() {
            Some(parse_duration(&timeout_val, ctx)?)
        } else {
            None
        }
    } else {
        None
    };

    Ok(Some(SubmitConfig { timeout }))
}

/// Parse duration from JS value.
fn parse_duration(value: &JsValue, ctx: &mut Context) -> JsResult<Duration> {
    let obj = value
        .as_object()
        .ok_or_else(|| JsNativeError::typ().with_message("duration must be an object"))?;

    if let Ok(ms) = obj.get(js_string!("milliseconds"), ctx)
        && !ms.is_undefined()
    {
        return Ok(Duration::Milliseconds(ms.to_u32(ctx)? as u64));
    }

    if let Ok(secs) = obj.get(js_string!("seconds"), ctx)
        && !secs.is_undefined()
    {
        return Ok(Duration::Seconds(secs.to_u32(ctx)? as u64));
    }

    if let Ok(mins) = obj.get(js_string!("minutes"), ctx)
        && !mins.is_undefined()
    {
        return Ok(Duration::Minutes(mins.to_u32(ctx)?));
    }

    if let Ok(hours) = obj.get(js_string!("hours"), ctx)
        && !hours.is_undefined()
    {
        return Ok(Duration::Hours(hours.to_u32(ctx)?));
    }

    if let Ok(days) = obj.get(js_string!("days"), ctx)
        && !days.is_undefined()
    {
        return Ok(Duration::Days(days.to_u32(ctx)?));
    }

    Err(JsNativeError::typ()
        .with_message("duration must have milliseconds, seconds, minutes, hours, or days")
        .into())
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
