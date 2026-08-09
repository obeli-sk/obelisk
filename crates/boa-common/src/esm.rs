//! ES Module helper for Boa JS runtimes.
//!
//! Provides utilities for parsing and evaluating ES modules with `export default`.

use crate::wasi_job_executor::WasiJobExecutor;
use boa_engine::{
    Context, JsError, JsResult, JsValue,
    builtins::promise::PromiseState,
    module::Module,
    object::builtins::{JsFunction, JsPromise},
};
use std::cell::RefCell;
use std::rc::Rc;

/// Errors that can occur when loading or evaluating an ES module.
#[derive(Debug)]
pub enum EsmError {
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

/// Load, link, evaluate, and extract the default export from a pre-parsed module.
///
/// Use this when you already have a `Module` (e.g. obtained by registering a
/// multi-file graph via [`crate::graph::register_source_modules`]).
pub async fn get_default_export_from_module(
    module: Module,
    context: &RefCell<&mut Context>,
    executor: &Rc<WasiJobExecutor>,
) -> Result<JsFunction, EsmError> {
    // Load module dependencies
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

    // Link the module
    module
        .link(*context.borrow_mut())
        .map_err(|err| EsmError::from_js_error(err, EsmError::LinkError))?;

    // Evaluate the module
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

    // Get the module namespace and extract the default export
    let namespace = module.namespace(*context.borrow_mut());
    let default_export = namespace
        .get(boa_engine::js_string!("default"), *context.borrow_mut())
        .map_err(|err| EsmError::from_js_error(err, EsmError::EvalError))?;

    // Check if default export exists
    if default_export.is_undefined() {
        return Err(EsmError::NoDefaultExport);
    }

    // Verify it's a callable function
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
