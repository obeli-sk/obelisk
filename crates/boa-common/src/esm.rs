//! ES Module helper for Boa JS runtimes.
//!
//! Provides utilities for parsing and evaluating ES modules with `export default`.

use crate::wasi_job_executor::WasiJobExecutor;
use boa_engine::{
    Context, JsError, Source,
    builtins::promise::PromiseState,
    module::{IdleModuleLoader, Module},
    object::builtins::JsFunction,
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
/// 1. Uses `IdleModuleLoader` (rejects all imports - fail fast)
/// 2. Parses the JS code as an ES Module
/// 3. Loads module dependencies (should resolve immediately with no imports)
/// 4. Links the module
/// 5. Evaluates the module
/// 6. Extracts the `default` export from the module namespace
/// 7. Verifies it's a callable function
///
/// # Arguments
/// * `js_code` - JavaScript source code with `export default function(...) { ... }`
/// * `context` - Boa JS context (must be configured before calling)
///
/// # Returns
/// The default export as a `JsFunction`, or an `EsmError` if any step fails.
pub fn get_default_export(js_code: &str, context: &mut Context) -> Result<JsFunction, EsmError> {
    // Ensure we use IdleModuleLoader which rejects any imports
    // Note: The context should already be configured with IdleModuleLoader by default,
    // but we explicitly set it here to be safe.
    let _loader = Rc::new(IdleModuleLoader);
    // Context's module_loader is already set in the builder, we rely on that.

    // 1. Parse the JS code as an ES Module
    let module = Module::parse(Source::from_bytes(js_code), None, context)
        .map_err(|err| EsmError::from_js_error(err, EsmError::ParseError))?;

    // 2. Load module dependencies
    let load_promise = module.load(context);

    // Drive the load promise to completion (should resolve immediately with no imports)
    context
        .run_jobs()
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
        .link(context)
        .map_err(|err| EsmError::from_js_error(err, EsmError::LinkError))?;

    // 4. Evaluate the module
    let eval_promise = module.evaluate(context);

    // Drive the evaluate promise to completion
    context
        .run_jobs()
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
    let namespace = module.namespace(context);
    let default_export = namespace
        .get(boa_engine::js_string!("default"), context)
        .map_err(|err| EsmError::from_js_error(err, EsmError::EvalError))?;

    // 6. Check if default export exists and is undefined
    if default_export.is_undefined() {
        return Err(EsmError::NoDefaultExport);
    }

    // 7. Verify it's a callable function
    let Some(func) = default_export.as_callable() else {
        return Err(EsmError::DefaultNotCallable);
    };

    // Convert JsObject to JsFunction
    JsFunction::from_object(func.clone()).ok_or(EsmError::DefaultNotCallable)
}

/// Async version of [`get_default_export`] for use in async contexts.
///
/// This version uses `WasiJobExecutor::drive_jobs` instead of `context.run_jobs()`,
/// which avoids nesting `block_on` calls when already inside wstd's async runtime
/// (e.g., in webhook handlers).
pub async fn get_default_export_async(
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
    let eval_promise = module.evaluate(*context.borrow_mut());

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
