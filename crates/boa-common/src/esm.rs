//! ES Module helper for Boa JS runtimes.
//!
//! Provides utilities for parsing and evaluating ES modules with `export default`.

use crate::wasi_job_executor::WasiJobExecutor;
use boa_engine::{
    Context, JsError, JsResult, JsValue, Source,
    builtins::promise::PromiseState,
    module::Module,
    object::builtins::{JsFunction, JsPromise},
};
use std::cell::RefCell;
use std::rc::Rc;

/// Errors that can occur when loading or evaluating an ES module.
#[derive(Debug)]
pub enum EsmError {
    /// Module source code could not be parsed.
    ParseError(String),
    /// Module could not be loaded (e.g., import resolution failed).
    LoadError(String),
    /// Module could not be linked.
    LinkError(String),
    /// Module evaluation failed.
    EvalError(String),
    /// Module does not have a default export.
    NoDefaultExport,
    /// Default export is not a callable function.
    DefaultNotCallable,
}

impl EsmError {
    /// Convert a `JsError` to an `EsmError` with a given variant constructor.
    fn from_js_error(err: JsError, f: impl FnOnce(String) -> Self) -> Self {
        f(err.to_string())
    }
}

/// Parse an ES module and extract its default export as a callable function.
///
/// This function:
/// 1. Parses the JS code as an ES Module
/// 2. Loads module dependencies (should resolve immediately with no imports)
/// 3. Links the module
/// 4. Evaluates the module
/// 5. Extracts the `default` export from the module namespace
/// 6. Verifies it's a callable function
///
/// # Arguments
/// * `js_code` - JavaScript source code with `export default function(...) { ... }`
/// * `context` - Boa JS context wrapped in RefCell
/// * `executor` - The WasiJobExecutor for driving async jobs
///
/// # Returns
/// The default export as a `JsFunction`, or an `EsmError` if any step fails.
pub async fn get_default_export(
    js_code: &str,
    context: &RefCell<&mut Context>,
    executor: &Rc<WasiJobExecutor>,
) -> Result<JsFunction, EsmError> {
    // 1. Parse the JS code as an ES Module
    let module = Module::parse(Source::from_bytes(js_code), None, *context.borrow_mut())
        .map_err(|err| EsmError::from_js_error(err, EsmError::ParseError))?;

    // 2. Load module dependencies
    let load_promise = module.load(*context.borrow_mut());

    // Drive the load promise to completion using async executor
    executor
        .clone()
        .drive_jobs(context)
        .await
        .map_err(|err| EsmError::from_js_error(err, EsmError::LoadError))?;

    match load_promise.state() {
        PromiseState::Fulfilled(_) => {}
        PromiseState::Rejected(err) => {
            return Err(EsmError::LoadError(JsError::from_opaque(err).to_string()));
        }
        PromiseState::Pending => {
            return Err(EsmError::LoadError(
                "module load promise is still pending".to_string(),
            ));
        }
    }

    // 3. Link the module
    module
        .link(*context.borrow_mut())
        .map_err(|err| EsmError::from_js_error(err, EsmError::LinkError))?;

    // 4. Evaluate the module
    let eval_promise = module
        .evaluate(*context.borrow_mut())
        .map_err(|err| EsmError::EvalError(err.to_string()))?;

    // Drive the evaluate promise to completion using async executor
    executor
        .clone()
        .drive_jobs(context)
        .await
        .map_err(|err| EsmError::from_js_error(err, EsmError::EvalError))?;

    match eval_promise.state() {
        PromiseState::Fulfilled(_) => {}
        PromiseState::Rejected(err) => {
            return Err(EsmError::EvalError(JsError::from_opaque(err).to_string()));
        }
        PromiseState::Pending => {
            return Err(EsmError::EvalError(
                "module evaluate promise is still pending".to_string(),
            ));
        }
    }

    // 5. Get the module namespace and extract the default export
    let namespace = module.namespace(*context.borrow_mut());
    let default_export = namespace
        .get(boa_engine::js_string!("default"), *context.borrow_mut())
        .map_err(|err| EsmError::from_js_error(err, EsmError::EvalError))?;

    // 6. Check if default export exists
    if default_export.is_undefined() {
        return Err(EsmError::NoDefaultExport);
    }

    // 7. Verify it's a callable function
    let Some(func) = default_export.as_callable() else {
        return Err(EsmError::DefaultNotCallable);
    };

    JsFunction::from_object(func.clone()).ok_or(EsmError::DefaultNotCallable)
}

/// If `value` is a Promise, drive it to completion and return the resolved value.
///
/// This function drives the executor until the specific promise resolves,
/// then returns immediately (abandoning any orphaned jobs like unwaited timers).
pub async fn resolve_promise(
    value: &JsValue,
    context: &RefCell<&mut Context>,
    executor: &Rc<WasiJobExecutor>,
) -> JsResult<JsValue> {
    let Some(object) = value.as_object() else {
        return Ok(value.clone());
    };
    let Ok(promise) = JsPromise::from_object(object) else {
        return Ok(value.clone());
    };

    // Drive jobs until this specific promise resolves, then stop immediately.
    // This abandons orphaned jobs (like unwaited setTimeout callbacks).
    executor
        .clone()
        .drive_jobs_until(context, || {
            !matches!(promise.state(), PromiseState::Pending)
        })
        .await?;

    // Return the resolved value
    match promise.state() {
        PromiseState::Fulfilled(v) => Ok(v),
        PromiseState::Rejected(e) => Err(JsError::from_opaque(e)),
        PromiseState::Pending => unreachable!("promise should be resolved after drive_jobs_until"),
    }
}
