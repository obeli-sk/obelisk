//! JavaScript runtime using Boa engine for Obelisk JS activities.
//!
//! This runtime provides:
//! - `console.*` → `obelisk:log` routing
//! - `fetch()` → WASIp2 HTTP outgoing requests
//! - `process.env["MY_VAR"]` → environment variable access
//! - ES Module support with `export default`
//!
//! # JS API Reference
//!
//! ## Environment Variables
//! ```js
//! // Get environment variable value (returns string or undefined)
//! const value = process.env["MY_VAR"];
//! ```
//!
//! ## Console Logging
//! ```js
//! console.log("info message");
//! console.debug("debug message");
//! console.warn("warning");
//! console.error("error");
//! ```

use crate::generated::obelisk::log::log as obelisk_log;
use crate::generated::{
    exports::obelisk_activity::activity_js_runtime::execute::JsRuntimeError,
    obelisk::log::log::error as host_fn_error,
};
use boa_common::console::{ObeliskLogger, setup_console};
use boa_common::esm::{EsmError, get_default_export, resolve_promise};
use boa_common::wasi_fetcher::WasiFetcher;
use boa_common::wasi_job_executor::WasiJobExecutor;
use boa_engine::{Context, JsResult, JsValue, Source};
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

    // Run the async execution inside a single wstd reactor
    wstd::runtime::block_on(execute_async(js_code, params_json, &mut context, &executor))
}

/// Async implementation of JS execution.
async fn execute_async(
    js_code: &str,
    params_json: &[String],
    context: &mut Context,
    executor: &Rc<WasiJobExecutor>,
) -> Result<Result<String, String>, JsRuntimeError> {
    let context = RefCell::new(context);

    // Get the default export function from the ES module
    let default_fn = match get_default_export(js_code, &context, executor).await {
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
                .borrow_mut()
                .eval(Source::from_bytes(param))
                .expect("already verified that params_json elements are parseable")
        })
        .collect();

    // Call the default export function with the params
    let result = default_fn.call(&JsValue::undefined(), &args, *context.borrow_mut());

    // If the result is a Promise, drive it to completion.
    let result = match result {
        Ok(ref js_value) => resolve_promise(js_value, &context, executor).await,
        err => err,
    };

    convert_result(result, &context)
}

/// Convert JS result to Rust result.
///
/// The ok value is JSON-serialized so the JS worker can deserialize it as any configured type.
fn convert_result(
    result: JsResult<JsValue>,
    context: &RefCell<&mut Context>,
) -> Result<Result<String, String>, JsRuntimeError> {
    match result {
        Ok(js_value) => {
            match js_value.to_json(*context.borrow_mut()) {
                Ok(Some(json_val)) => Ok(Ok(serde_json::to_string(&json_val)
                    .expect("serde_json::Value must be serializable"))),
                Ok(None) => Ok(Ok("null".to_string())), // undefined → null
                Err(e) => Err(JsRuntimeError::WrongReturnType(format!(
                    "cannot serialize to JSON: {e:?}"
                ))),
            }
        }
        Err(js_err) => {
            // JSON-encode the thrown value (consistent with ok branch).
            // `throw new Error("msg")` → JSON-encode the message string.
            // `throw expr` (null, string, number, object, …) → serialize via to_json.
            let err_json = if let Ok(native_err) = js_err.try_native(*context.borrow_mut()) {
                serde_json::to_string(&native_err.message().to_string())
                    .expect("string serialization is infallible")
            } else {
                let opaque = js_err
                    .as_opaque()
                    .expect("non-native JsError must be opaque");
                match opaque.to_json(*context.borrow_mut()) {
                    Ok(Some(json_val)) => serde_json::to_string(&json_val)
                        .expect("serde_json::Value must be serializable"),
                    Ok(None) => "null".to_string(), // undefined → null
                    Err(e) => {
                        return Err(JsRuntimeError::WrongThrownType(format!(
                            "cannot serialize thrown value to JSON: {e:?}"
                        )));
                    }
                }
            };
            Ok(Err(err_json))
        }
    }
}

/// Register the `fetch` API backed by WASIp2 HTTP.
fn setup_fetch(context: &mut Context) -> JsResult<()> {
    boa_runtime::register(FetchExtension(WasiFetcher), None, context)
}
