//! JavaScript runtime using Boa engine for Obelisk JS activities.
//!
//! This runtime provides:
//! - `console.*` → `obelisk:log` routing
//! - `fetch()` → WASIp2 HTTP outgoing requests

use crate::generated::obelisk::log::log as obelisk_log;
use crate::generated::{
    exports::obelisk_activity::activity_js_runtime::execute::JsRuntimeError,
    obelisk::log::log::error as host_fn_error,
};
use crate::wasi_fetcher::WasiFetcher;
use crate::wasi_job_executor::WasiJobExecutor;
use boa_engine::{
    Context, JsError, JsNativeError, JsObject, JsResult, JsValue, NativeFunction, Source,
    builtins::promise::PromiseState, js_string, object::builtins::JsPromise,
    property::Attribute,
};
use boa_runtime::extensions::FetchExtension;
use std::cell::RefCell;
use std::rc::Rc;

/// Execute JavaScript code with the given parameters.
///
/// `params_json` is expected to be a JSON array string. The array is passed
/// as the first and only argument to `fn_name`.
pub fn execute(
    fn_name: &str,
    js_code: &str,
    params_json: &str,
) -> Result<Result<String, String>, JsRuntimeError> {
    // `fn_name` comes from trusted `activity_js_worker`, must be FFQN's fn name
    let fn_name = fn_name.replace('-', "_");

    let executor = Rc::new(WasiJobExecutor::default());
    let mut context = Context::builder()
        .job_executor(executor.clone())
        .build()
        .expect("building context must work");

    // Set up console
    setup_console(&mut context).expect("console setup must work");

    // Set up fetch
    setup_fetch(&mut context).expect("fetch setup must work");

    // `params_json` is sent by trusted `activity_js_worker`, params were typechecked.
    // Store as global `__params__` array.
    // Direct interpolation,  JSON array/object literals are valid JavaScript syntax.
    let params_code = format!("const __params__ = {params_json};");
    context
        .eval(Source::from_bytes(&params_code))
        .expect("already verified that params_json is parseable");

    // Add the function to the context, without running it.
    let bare_fn_eval = context.eval(Source::from_bytes(js_code));
    if let Err(err) = bare_fn_eval {
        host_fn_error("cannot evaluate - {err:?}"); // Send additional info via obelisk:log
        return Err(JsRuntimeError::CannotDeclareFunction(err.to_string()));
    }

    let typeof_fn = context.eval(Source::from_bytes(&format!("typeof {fn_name}")));
    let Ok(typeof_fn) = typeof_fn else {
        return Err(JsRuntimeError::FunctionNotFound);
    };
    let Some(typeof_fn) = typeof_fn.as_string() else {
        return Err(JsRuntimeError::FunctionNotFound);
    };
    if typeof_fn.as_str() != "function" {
        return Err(JsRuntimeError::FunctionNotFound);
    }

    let call_fn = format!("{fn_name}(__params__);");

    let result = context.eval(Source::from_bytes(&call_fn));

    // If the result is a Promise, drive it to completion.
    let result = match result {
        Ok(ref js_value) => resolve_if_promise(js_value, &mut context, &executor),
        err => err,
    };

    match result {
        Ok(js_value) => {
            if let Some(string) = js_value.as_string() {
                Ok(Ok(string.to_std_string_escaped()))
            } else {
                Err(JsRuntimeError::WrongReturnType(format!(
                    "expected string, got {js_value:?}"
                )))
            }
        }
        Err(js_err) => {
            // Extract error message: try opaque string, then native error message.
            if let Some(msg) = extract_error_string(&js_err) {
                Ok(Err(msg))
            } else {
                Err(JsRuntimeError::WrongThrownType(format!(
                    "expected string, got {js_err:?}"
                )))
            }
        }
    }
}

/// Extract a string message from a `JsError`.
///
/// Tries in order:
/// 1. Opaque error that is a JS string → use the string directly
/// 2. Native error (e.g. from `JsNativeError::error().with_message(...)`) → use `.message()`
/// 3. Otherwise → `None`
fn extract_error_string(err: &boa_engine::JsError) -> Option<String> {
    // 1. Opaque string (user threw a string: `throw "some error"`)
    if let Some(js_value) = err.as_opaque() {
        if let Some(string) = js_value.as_string() {
            return Some(string.to_std_string_escaped());
        }
    }
    // 2. Native error (from JsNativeError, our WasiFetcher errors, etc.)
    if let Some(native) = err.as_native() {
        let msg = native.message().to_string();
        if !msg.is_empty() {
            return Some(msg);
        }
    }
    None
}

/// If `value` is a Promise, drive it to completion and return the resolved value.
///
/// We avoid [`JsPromise::await_blocking`] because it creates a tight loop calling
/// `run_jobs()` → `wstd::runtime::block_on()` repeatedly. Each `block_on` creates a
/// new wstd reactor that exits immediately when no pollables are pending (e.g. when
/// only synchronous promise microtasks remain). This busy-loop starves the tokio
/// runtime, preventing wiremock (or any other async task) from running on
/// single-threaded tokio.
///
/// Instead, we drive the entire promise resolution inside a **single**
/// `wstd::runtime::block_on` call. The wstd reactor persists across all job iterations,
/// so WASIp2 pollables registered by `fetch()` are properly tracked and the reactor
/// blocks on them via `wasi:io/poll::poll`, yielding the wasmtime fiber to tokio.
fn resolve_if_promise(
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

    // Drive promise resolution inside a single wstd reactor.
    let executor = executor.clone();
    wstd::runtime::block_on(async {
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
    })
}

/// Register the `fetch` API backed by WASIp2 HTTP.
fn setup_fetch(context: &mut Context) -> JsResult<()> {
    boa_runtime::register(FetchExtension(WasiFetcher), None, context)
}

/// Set up the global `console` object routing to obelisk:log.
fn setup_console(context: &mut Context) -> JsResult<()> {
    let console = JsObject::default(context.intrinsics());

    macro_rules! console_method {
        ($name:expr, $log_fn:path) => {{
            let func = NativeFunction::from_fn_ptr(|_this, args, ctx| {
                let msg = console_args_to_string(args, ctx)?;
                $log_fn(&msg);
                Ok(JsValue::undefined())
            });
            console.set(
                js_string!($name),
                func.to_js_function(context.realm()),
                false,
                context,
            )?;
        }};
    }

    console_method!("trace", obelisk_log::trace);
    console_method!("debug", obelisk_log::debug);
    console_method!("log", obelisk_log::info);
    console_method!("info", obelisk_log::info);
    console_method!("warn", obelisk_log::warn);
    console_method!("error", obelisk_log::error);

    context.register_global_property(js_string!("console"), console, Attribute::all())?;
    Ok(())
}

/// Convert function arguments to a space-separated string.
fn console_args_to_string(args: &[JsValue], ctx: &mut Context) -> JsResult<String> {
    let parts: Result<Vec<String>, _> = args
        .iter()
        .map(|v| {
            if let Some(s) = v.as_string() {
                Ok(s.to_std_string_escaped())
            } else {
                // Try to stringify objects
                console_json_stringify(v, ctx)
                    .or_else(|_| v.to_string(ctx).map(|s| s.to_std_string_escaped()))
            }
        })
        .collect();

    Ok(parts?.join(" "))
}

/// Convert JS value to JSON string using the built-in JSON.stringify.
fn console_json_stringify(value: &JsValue, ctx: &mut Context) -> JsResult<String> {
    let json = ctx.global_object().get(js_string!("JSON"), ctx)?;
    let json_obj = json
        .as_object()
        .ok_or_else(|| JsNativeError::error().with_message("JSON global not found"))?;
    let stringify = json_obj.get(js_string!("stringify"), ctx)?;
    let stringify_fn = stringify
        .as_callable()
        .ok_or_else(|| JsNativeError::error().with_message("JSON.stringify not callable"))?;

    let result = stringify_fn.call(&json, std::slice::from_ref(value), ctx)?;

    result
        .as_string()
        .map(|s| s.to_std_string_escaped())
        .ok_or_else(|| {
            JsNativeError::error()
                .with_message("JSON.stringify returned non-string")
                .into()
        })
}
