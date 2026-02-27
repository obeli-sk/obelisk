//! JavaScript runtime using Boa engine for Obelisk JS activities.
//!
//! This runtime provides:
//! - `console.*` → `obelisk:log` routing
//! - `fetch()` → WASIp2 HTTP outgoing requests
//! - ES Module support with `export default`

use crate::generated::obelisk::log::log as obelisk_log;
use crate::generated::{
    exports::obelisk_activity::activity_js_runtime::execute::JsRuntimeError,
    obelisk::log::log::error as host_fn_error,
};
use boa_common::console::{ObeliskLogger, setup_console};
use boa_common::esm::{EsmError, get_default_export};
use boa_common::wasi_fetcher::WasiFetcher;
use boa_common::wasi_job_executor::WasiJobExecutor;
use boa_engine::{
    Context, JsError, JsResult, JsValue, Source, builtins::promise::PromiseState,
    object::builtins::JsPromise,
};
use boa_runtime::extensions::FetchExtension;
use std::cell::RefCell;
use std::rc::Rc;

/// Logger implementation using the generated obelisk:log bindings.
#[derive(Clone, Copy)]
struct Logger;

impl ObeliskLogger for Logger {
    fn trace(&self, msg: &str) {
        obelisk_log::trace(msg);
    }
    fn debug(&self, msg: &str) {
        obelisk_log::debug(msg);
    }
    fn info(&self, msg: &str) {
        obelisk_log::info(msg);
    }
    fn warn(&self, msg: &str) {
        obelisk_log::warn(msg);
    }
    fn error(&self, msg: &str) {
        obelisk_log::error(msg);
    }
}

/// Execute JavaScript code with the given parameters.
///
/// `params_json` is a list of JSON-serialized parameter values.
/// Each element is passed as a positional argument to the default export function.
pub fn execute(
    js_code: &str,
    params_json: &[String],
) -> Result<Result<String, String>, JsRuntimeError> {
    let executor = Rc::new(WasiJobExecutor::default());
    let mut context = Context::builder()
        .job_executor(executor.clone())
        .build()
        .expect("building context must work");

    // Set up console
    setup_console(&mut context, Logger).expect("console setup must work");

    // Set up fetch
    setup_fetch(&mut context).expect("fetch setup must work");

    // Get the default export function from the ES module
    let default_fn = match get_default_export(js_code, &mut context) {
        Ok(func) => func,
        Err(EsmError::ParseError(msg)) => {
            host_fn_error(&format!("module parse error: {msg}"));
            return Err(JsRuntimeError::ModuleParseError(msg));
        }
        Err(EsmError::LoadError(msg)) => {
            host_fn_error(&format!("module load error: {msg}"));
            return Err(JsRuntimeError::ModuleParseError(msg));
        }
        Err(EsmError::LinkError(msg)) => {
            host_fn_error(&format!("module link error: {msg}"));
            return Err(JsRuntimeError::ModuleParseError(msg));
        }
        Err(EsmError::EvalError(msg)) => {
            host_fn_error(&format!("module eval error: {msg}"));
            return Err(JsRuntimeError::ModuleParseError(msg));
        }
        Err(EsmError::NoDefaultExport | EsmError::DefaultNotCallable) => {
            return Err(JsRuntimeError::NoDefaultExport);
        }
    };

    // `params_json` is sent by trusted `activity_js_worker`, params were typechecked.
    // Parse each JSON param into a JsValue.
    let args: Vec<JsValue> = params_json
        .iter()
        .map(|param| {
            // Each param is a JSON value — parse it as a JS value
            context
                .eval(Source::from_bytes(param))
                .expect("already verified that params_json elements are parseable")
        })
        .collect();

    // Call the default export function with the params
    let result = default_fn.call(&JsValue::undefined(), &args, &mut context);

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
            if let Ok(native_err) = js_err.try_native(&mut context) {
                // `throw new Error('foo')` goes here
                Ok(Err(native_err.message().to_string()))
            } else if let Some(err_str) = extract_error_string(&js_err) {
                Ok(Err(err_str))
            } else {
                Err(JsRuntimeError::WrongThrownType(format!(
                    "expected string, got {js_err:?}"
                )))
            }
        }
    }
}

/// Extract a string message from a `JsError`.
fn extract_error_string(err: &boa_engine::JsError) -> Option<String> {
    if let Some(js_value) = err.as_opaque()
        && let Some(string) = js_value.as_string()
    {
        // `throw 'string'` goes here
        let string = string.to_std_string_escaped();
        return Some(string);
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
