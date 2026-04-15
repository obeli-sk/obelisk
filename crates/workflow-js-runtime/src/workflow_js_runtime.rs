//! JavaScript runtime using Boa engine for Obelisk JS workflows.
//!
//! This runtime provides:
//! - `obelisk.*` workflow API (join sets, sleep, random, etc.)
//! - `console.*` → `obelisk:log` routing
//! - ES Module support with `export default`
//!
//! Unlike activity-js-runtime, this runtime:
//! - Does NOT provide `fetch()` (HTTP is not deterministic)
//! - Uses synchronous execution only (no async/promises)
//! - Runs on `wasm32-unknown-unknown` target
//!
//! # JS API Reference
//!
//! ## Basic Workflow
//! ```js
//! export default function myWorkflow(params) {
//!     // params is the input array passed to the workflow
//!     return "result"; // Return value (or throw for error)
//! }
//! ```
//!
//! ## Join Sets (Child Executions)
//! ```js
//! // Create a join set to manage child executions
//! const js = obelisk.createJoinSet();
//! // Or with a name: obelisk.createJoinSet("my-join-set")
//!
//! // Submit a child execution
//! const execId = js.submit("ns:pkg/ifc.func", [arg1, arg2]);
//!
//! // Wait for next result (blocks until a child completes)
//! const response = js.joinNext();
//! // response = { type: "execution"|"delay", id: string, ok: boolean }
//!
//! // Get the actual result value
//! const result = obelisk.getResult(execId);
//! // result = { ok: value } or { err: value }
//!
//! // Close the join set when done
//! js.close();
//! ```
//!
//! ## Scheduling Top-Level Executions
//! ```js
//! // Generate execution ID first
//! const execId = obelisk.executionIdGenerate();
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
//! ## Stubbing (Mock Responses)
//! ```js
//! // Stub a child execution with a predetermined result
//! const js = obelisk.createJoinSet();
//! const execId = js.submit("ns:pkg/activity.stub_fn", [args]);
//! obelisk.stub(execId, { ok: "mocked-value" }); // or { err: "error" }
//! ```
//!
//! ## Sleep (Durable Timer)
//! ```js
//! // Sleep for a duration (persisted, survives restarts)
//! // Returns a Date object representing the wake-up time.
//! const wakeUp = obelisk.sleep({ seconds: 30 });  // Date
//! const wakeUp = obelisk.sleep({ minutes: 5 });   // Date
//! const wakeUp = obelisk.sleep({ milliseconds: 500 }); // Date
//! console.log(wakeUp.toISOString()); // e.g. "2024-01-01T00:00:00.500Z"
//! ```
//!
//! ## Random (Deterministic)
//! ```js
//! // Standard JS Math.random() — deterministic, backed by obelisk.randomU64
//! const r = Math.random(); // [0, 1)
//!
//! // Random integers (deterministic replay)
//! const n = obelisk.randomU64(0, 100);        // [0, 100)
//! const m = obelisk.randomU64Inclusive(1, 6); // [1, 6] (dice roll)
//!
//! // Random string (alphanumeric)
//! const s = obelisk.randomString(8, 16); // length in [8, 16)
//! ```
//!
//! ## Delays in Join Sets
//! ```js
//! const js = obelisk.createJoinSet();
//! const execId = js.submit("ns:pkg/ifc.slow_task", []);
//! const delayId = js.submitDelay({ seconds: 30 }); // timeout
//!
//! const response = js.joinNext();
//! if (response.type === "delay") {
//!     // Timeout fired before task completed
//! } else {
//!     // Task completed
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

use crate::deterministic_executor::DeterministicJobExecutor;
use crate::generated::exports::obelisk_workflow::workflow_js_runtime::execute::JsRuntimeError;
use crate::generated::obelisk::log::log as obelisk_log;
use crate::generated::obelisk::types::backtrace::{FrameInfo, FrameSymbol, WasmBacktrace};
use crate::generated::obelisk::types::execution::{ExecutionId, Function, ResponseId};
use crate::generated::obelisk::types::join_set::JoinSet;
use crate::generated::obelisk::types::time::{Datetime, Duration, ScheduleAt};
use crate::generated::obelisk::workflow::workflow_support::{
    JoinNextTryError, SubmitConfig, call_json, execution_id_generate, get_result_json_bt,
    join_next_bt, join_next_try_bt, join_set_close_bt, join_set_create_bt,
    join_set_create_named_bt, random_string_bt, random_u64_bt, random_u64_inclusive_bt,
    schedule_json, sleep_bt, stub_json, submit_delay_bt, submit_json_bt,
};
use boa_common::console::{ObeliskLogger, json_stringify, setup_console};
use boa_common::helpers::{new_object, parse_ffqn};
use boa_engine::context::time::FixedClock;
use boa_engine::{
    Context, JsArgs, JsError, JsNativeError, JsResult, JsValue, Module, NativeFunction, Source,
    builtins::promise::PromiseState,
    js_string,
    object::builtins::{JsDate, JsFunction},
    property::Attribute,
};
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

// Thread-local storage for JoinSet resources and the JS file name (WASM is single-threaded)
thread_local! {
    static JOIN_SETS: RefCell<Vec<Option<JoinSet>>> = const { RefCell::new(Vec::new()) };
    static JS_FILE_NAME: RefCell<String> = const { RefCell::new(String::new()) };
}

fn store_join_set(js: JoinSet) -> usize {
    JOIN_SETS.with(|sets| {
        let mut sets = sets.borrow_mut();
        let idx = sets.len();
        sets.push(Some(js));
        idx
    })
}

fn with_join_set<T, F: FnOnce(&JoinSet) -> T>(idx: usize, f: F) -> Result<T, JsNativeError> {
    JOIN_SETS
        .with(|sets| {
            let sets = sets.borrow();
            sets.get(idx).and_then(|opt| opt.as_ref()).map(f)
        })
        .ok_or_else(|| JsNativeError::error().with_message("JoinSet has been closed"))
}

fn take_join_set(idx: usize) -> Option<JoinSet> {
    JOIN_SETS.with(|sets| {
        let mut sets = sets.borrow_mut();
        sets.get_mut(idx).and_then(|opt| opt.take())
    })
}

/// Capture the current Boa JS stack trace as a `WasmBacktrace`.
///
/// The `module` and `file` fields are populated from `JS_FILE_NAME` (set from
/// the `js-file-name` parameter passed to `execute`), since JS source is loaded
/// from memory and Boa does not track a file path for in-memory sources.
fn capture_backtrace(ctx: &Context) -> WasmBacktrace {
    use boa_engine::vm::SourcePath;
    let js_file_name = JS_FILE_NAME.with(|s| s.borrow().clone());
    let js_file_name_opt: Option<&str> = if js_file_name.is_empty() {
        None
    } else {
        Some(&js_file_name)
    };
    let frames = ctx
        .stack_trace()
        .map(|frame| {
            let loc = frame.position();
            let module = match &loc.path {
                SourcePath::Path(p) => p.display().to_string(),
                _ => js_file_name_opt
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| "unknown".to_string()),
            };
            let file = match &loc.path {
                SourcePath::Path(p) => Some(p.display().to_string()),
                _ => js_file_name_opt.map(|s| s.to_string()),
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

/// Execute JavaScript code with the given parameters.
///
/// `js_file_name` is the source file name used in backtraces (from `__OBELISK_JS_FILE_NAME__`).
/// `params_json` is a list of JSON-serialized parameter values.
/// Each element is passed as a positional argument to the default export function.
pub fn execute(
    js_code: &str,
    params_json: &[String],
    js_file_name: Option<String>,
) -> Result<Result<String, String>, JsRuntimeError> {
    if let Some(js_file_name) = js_file_name {
        JS_FILE_NAME.with(|s| *s.borrow_mut() = js_file_name);
    }

    let executor = Rc::new(DeterministicJobExecutor::default());
    let mut context = Context::builder()
        .job_executor(executor.clone())
        .clock(Rc::new(FixedClock::from_millis(0)))
        .build()
        .expect("building context must work");

    // Set up the obelisk global object with workflow APIs BEFORE module evaluation
    // so that `obelisk.*` is available during module initialization
    setup_obelisk_api(&mut context).expect("obelisk API setup must work");

    // Set up console
    setup_console(&mut context, Logger).expect("console setup must work");

    // Override Math.random() to use deterministic random source
    setup_math_random(&mut context).expect("Math.random setup must work");

    // Override Date.now() to use the Obelisk clock via sleep(0)
    setup_date_now(&mut context).expect("Date.now setup must work");

    // Get the default export function from the ES module
    let default_fn = match get_default_export_workflow(js_code, &mut context) {
        Ok(func) => func,
        Err(EsmErrorWorkflow::ParseError(msg)) => {
            obelisk_log::error(&format!("module parse error: {msg}"));
            return Err(JsRuntimeError::ModuleParseError(msg));
        }
        Err(EsmErrorWorkflow::LoadError(msg)) => {
            obelisk_log::error(&format!("module load error: {msg}"));
            return Err(JsRuntimeError::ModuleParseError(msg));
        }
        Err(EsmErrorWorkflow::LinkError(msg)) => {
            obelisk_log::error(&format!("module link error: {msg}"));
            return Err(JsRuntimeError::ModuleParseError(msg));
        }
        Err(EsmErrorWorkflow::EvalError(msg)) => {
            obelisk_log::error(&format!("module eval error: {msg}"));
            return Err(JsRuntimeError::ModuleParseError(msg));
        }
        Err(EsmErrorWorkflow::NoDefaultExport | EsmErrorWorkflow::DefaultNotCallable) => {
            return Err(JsRuntimeError::NoDefaultExport);
        }
    };

    // `params_json` is sent by trusted `workflow_js_worker`, params were typechecked.
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

    // JSON-serialize the return value so the worker can deserialize it as any configured type.
    match result {
        Ok(js_value) => {
            match js_value.to_json(&mut context) {
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
            let err_json = if let Ok(native_err) = js_err.try_native(&mut context) {
                serde_json::to_string(&native_err.message().to_string())
                    .expect("string serialization is infallible")
            } else {
                let opaque = js_err
                    .as_opaque()
                    .expect("non-native JsError must be opaque");
                match opaque.to_json(&mut context) {
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

/// Errors that can occur when loading or evaluating an ES module for workflows.
#[derive(Debug)]
enum EsmErrorWorkflow {
    ParseError(String),
    LoadError(String),
    LinkError(String),
    EvalError(String),
    NoDefaultExport,
    DefaultNotCallable,
}

impl EsmErrorWorkflow {
    fn from_js_error(err: JsError, f: impl FnOnce(String) -> Self) -> Self {
        f(err.to_string())
    }
}

/// Parse an ES module and extract its default export as a callable function.
///
/// This is a workflow-specific version that uses synchronous execution
/// (no job executor) since workflows don't use async/promises.
///
/// IMPORTANT: `obelisk.*` globals must be set up BEFORE calling this function
/// so they're available during module evaluation.
fn get_default_export_workflow(
    js_code: &str,
    context: &mut Context,
) -> Result<JsFunction, EsmErrorWorkflow> {
    // 1. Parse the JS code as an ES Module
    let module = Module::parse(Source::from_bytes(js_code), None, context)
        .map_err(|err| EsmErrorWorkflow::from_js_error(err, EsmErrorWorkflow::ParseError))?;

    // 2. Load module dependencies (should resolve immediately with no imports)
    let load_promise = module.load(context);
    context
        .run_jobs()
        .map_err(|err| EsmErrorWorkflow::from_js_error(err, EsmErrorWorkflow::LoadError))?;

    match load_promise.state() {
        PromiseState::Fulfilled(_) => {}
        PromiseState::Rejected(err) => {
            return Err(EsmErrorWorkflow::LoadError(
                JsError::from_opaque(err).to_string(),
            ));
        }
        PromiseState::Pending => {
            return Err(EsmErrorWorkflow::LoadError(
                "module load promise is still pending".to_string(),
            ));
        }
    }

    // 3. Link the module
    module
        .link(context)
        .map_err(|err| EsmErrorWorkflow::from_js_error(err, EsmErrorWorkflow::LinkError))?;

    // 4. Evaluate the module
    let eval_promise = module
        .evaluate(context)
        .map_err(|err| EsmErrorWorkflow::EvalError(err.to_string()))?;
    context
        .run_jobs()
        .map_err(|err| EsmErrorWorkflow::from_js_error(err, EsmErrorWorkflow::EvalError))?;

    match eval_promise.state() {
        PromiseState::Fulfilled(_) => {}
        PromiseState::Rejected(err) => {
            return Err(EsmErrorWorkflow::EvalError(
                JsError::from_opaque(err).to_string(),
            ));
        }
        PromiseState::Pending => {
            return Err(EsmErrorWorkflow::EvalError(
                "module evaluate promise is still pending".to_string(),
            ));
        }
    }

    // 5. Get the module namespace and extract the default export
    let namespace = module.namespace(context);
    let default_export = namespace
        .get(js_string!("default"), context)
        .map_err(|err| EsmErrorWorkflow::from_js_error(err, EsmErrorWorkflow::EvalError))?;

    // 6. Check if default export exists
    if default_export.is_undefined() {
        return Err(EsmErrorWorkflow::NoDefaultExport);
    }

    // 7. Verify it's a callable function
    let Some(func) = default_export.as_callable() else {
        return Err(EsmErrorWorkflow::DefaultNotCallable);
    };

    JsFunction::from_object(func.clone()).ok_or(EsmErrorWorkflow::DefaultNotCallable)
}

/// Set up the global `obelisk` object with workflow support functions.
fn setup_obelisk_api(context: &mut Context) -> JsResult<()> {
    let obelisk = new_object(context);

    // obelisk.createJoinSet([options])
    let create_join_set = NativeFunction::from_fn_ptr(|_this, args, ctx| {
        // Check for options.name
        if let Some(options) = args.first()
            && let Some(obj) = options.as_object()
            && let Ok(name_val) = obj.get(js_string!("name"), ctx)
            && let Some(name) = name_val.as_string()
        {
            let name_str = name.to_std_string_escaped();
            let backtrace = capture_backtrace(ctx);
            return match join_set_create_named_bt(&name_str, Some(&backtrace)) {
                Ok(js) => Ok(create_join_set_object(js, ctx)?),
                Err(e) => Err(JsNativeError::error()
                    .with_message(format!("Failed to create named join set: {:?}", e))
                    .into()),
            };
        }

        let backtrace = capture_backtrace(ctx);
        let js = join_set_create_bt(Some(&backtrace));
        create_join_set_object(js, ctx)
    });
    obelisk.set(
        js_string!("createJoinSet"),
        create_join_set.to_js_function(context.realm()),
        false,
        context,
    )?;

    // obelisk.sleep(schedule)
    let sleep_fn = NativeFunction::from_fn_ptr(|_this, args, ctx| {
        let schedule = parse_schedule_at(args.get_or_undefined(0), ctx)?;
        let backtrace = capture_backtrace(ctx);
        match sleep_bt(schedule, Some(&backtrace)) {
            Ok(dt) => {
                let ms = (dt.seconds as f64) * 1000.0 + (dt.nanoseconds as f64) / 1_000_000.0;
                let date = JsDate::new(ctx);
                date.set_time(ms, ctx)?;
                Ok(date.into())
            }
            Err(()) => Err(JsNativeError::error()
                .with_message("Sleep was cancelled")
                .into()),
        }
    });
    obelisk.set(
        js_string!("sleep"),
        sleep_fn.to_js_function(context.realm()),
        false,
        context,
    )?;

    // obelisk.randomU64(min, maxExclusive)
    // JS numbers are f64: inputs are exact up to Number.MAX_SAFE_INTEGER (2^53-1).
    // Since the return value is always < maxExclusive, it is also within safe range.
    let random_u64_fn = NativeFunction::from_fn_ptr(|_this, args, ctx| {
        let min = args.get_or_undefined(0).to_number(ctx)? as u64;
        let max = args.get_or_undefined(1).to_number(ctx)? as u64;
        let backtrace = capture_backtrace(ctx);
        let result = random_u64_bt(min, max, Some(&backtrace));
        Ok(JsValue::from(result))
    });
    obelisk.set(
        js_string!("randomU64"),
        random_u64_fn.to_js_function(context.realm()),
        false,
        context,
    )?;

    // obelisk.randomU64Inclusive(min, max)
    // Same f64 precision note as randomU64 above: result <= max, so always safe.
    let random_u64_inclusive_fn = NativeFunction::from_fn_ptr(|_this, args, ctx| {
        let min = args.get_or_undefined(0).to_number(ctx)? as u64;
        let max = args.get_or_undefined(1).to_number(ctx)? as u64;
        let backtrace = capture_backtrace(ctx);
        let result = random_u64_inclusive_bt(min, max, Some(&backtrace));
        Ok(JsValue::from(result))
    });
    obelisk.set(
        js_string!("randomU64Inclusive"),
        random_u64_inclusive_fn.to_js_function(context.realm()),
        false,
        context,
    )?;

    // obelisk.randomString(minLen, maxLenExcl)
    let random_string_fn = NativeFunction::from_fn_ptr(|_this, args, ctx| {
        let min_len = args.get_or_undefined(0).to_u32(ctx)? as u16;
        let max_len = args.get_or_undefined(1).to_u32(ctx)? as u16;
        let backtrace = capture_backtrace(ctx);
        let result = random_string_bt(min_len, max_len, Some(&backtrace));
        Ok(JsValue::from(js_string!(result)))
    });
    obelisk.set(
        js_string!("randomString"),
        random_string_fn.to_js_function(context.realm()),
        false,
        context,
    )?;

    // obelisk.call(ffqn, params, [config])
    // Convenience: createJoinSet → submit → joinNext → getResult → close, return result
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

        let backtrace = capture_backtrace(ctx);
        match call_json(&function, &params_json, config, Some(&backtrace)) {
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

    // obelisk.getResult(executionId)
    let get_result_fn = NativeFunction::from_fn_ptr(|_this, args, ctx| {
        let exec_id_str = args
            .get_or_undefined(0)
            .as_string()
            .ok_or_else(|| JsNativeError::typ().with_message("executionId must be a string"))?
            .to_std_string_escaped();

        let exec_id = ExecutionId { id: exec_id_str };
        let backtrace = capture_backtrace(ctx);

        match get_result_json_bt(&exec_id, Some(&backtrace)) {
            Ok(inner_result) => {
                let result_obj = new_object(ctx);
                match inner_result {
                    Ok(Some(json_str)) => {
                        // Parse the JSON result
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
                .with_message(format!("Failed to get result: {:?}", e))
                .into()),
        }
    });
    obelisk.set(
        js_string!("getResult"),
        get_result_fn.to_js_function(context.realm()),
        false,
        context,
    )?;

    // obelisk.executionIdGenerate()
    let exec_id_generate_fn = NativeFunction::from_fn_ptr(|_this, _args, ctx| {
        let backtrace = capture_backtrace(ctx);
        let exec_id = execution_id_generate(Some(&backtrace));
        Ok(JsValue::from(js_string!(exec_id.id)))
    });
    obelisk.set(
        js_string!("executionIdGenerate"),
        exec_id_generate_fn.to_js_function(context.realm()),
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

        let schedule = parse_schedule_at(args.get_or_undefined(3), ctx)?;

        let config = if let Some(cfg_val) = args.get(4) {
            parse_submit_config(cfg_val, ctx)?
        } else {
            None
        };

        let backtrace = capture_backtrace(ctx);
        match schedule_json(
            &exec_id,
            schedule,
            &function,
            &params_json,
            config,
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

    // obelisk.stub(executionId, result)
    let stub_fn = NativeFunction::from_fn_ptr(|_this, args, ctx| {
        let exec_id_str = args
            .get_or_undefined(0)
            .as_string()
            .ok_or_else(|| JsNativeError::typ().with_message("executionId must be a string"))?
            .to_std_string_escaped();
        let exec_id = ExecutionId { id: exec_id_str };

        let result_val = args.get_or_undefined(1);
        let result_json = json_stringify(result_val, ctx)?;

        let backtrace = capture_backtrace(ctx);
        match stub_json(&exec_id, &result_json, Some(&backtrace)) {
            Ok(()) => Ok(JsValue::undefined()),
            Err(e) => Err(JsNativeError::error()
                .with_message(format!("stub failed: {:?}", e))
                .into()),
        }
    });
    obelisk.set(
        js_string!("stub"),
        stub_fn.to_js_function(context.realm()),
        false,
        context,
    )?;

    // Set obelisk as global
    context.register_global_property(js_string!("obelisk"), obelisk, Attribute::all())?;

    Ok(())
}

/// Override `Math.random()` to use the deterministic Obelisk random source.
///
/// Standard `Math.random()` returns a value in `[0, 1)`. We emulate this by
/// generating a random u64 in `[0, 2^53)` and dividing by `2^53`.
fn setup_math_random(context: &mut Context) -> JsResult<()> {
    let math_rand_fn = NativeFunction::from_fn_ptr(|_this, _args, ctx| {
        let backtrace = capture_backtrace(ctx);
        // Generate a random value in [0, 2^53) and divide to get [0, 1).
        // 2^53 = 9007199254740992
        let max = 9007199254740992u64;
        let val = random_u64_bt(0, max, Some(&backtrace));
        // f64 is precise for all integers up to 2^53.
        let result = val as f64 / max as f64;
        Ok(JsValue::from(result))
    });

    let global = context.global_object();
    let math = global
        .get(js_string!("Math"), context)
        .expect("global Math object must be found");
    let math_obj = math.as_object().expect("global Math must be an object");
    math_obj.set(
        js_string!("random"),
        math_rand_fn.to_js_function(context.realm()),
        false,
        context,
    )?;
    Ok(())
}

/// Override `Date.now()` to return the current Obelisk clock time via `sleep_bt(0)`.
///
/// Returns milliseconds since Unix epoch as f64 (matching the JS spec).
fn setup_date_now(context: &mut Context) -> JsResult<()> {
    let date_now_fn = NativeFunction::from_fn_ptr(|_this, _args, ctx| {
        let backtrace = capture_backtrace(ctx);
        let dt = sleep_bt(ScheduleAt::Now, Some(&backtrace))
            .map_err(|()| JsNativeError::error().with_message("sleep failed"))?;
        let ms = (dt.seconds as f64) * 1000.0 + (dt.nanoseconds as f64) / 1_000_000.0;
        Ok(JsValue::from(ms))
    });

    let global = context.global_object();
    let date = global
        .get(js_string!("Date"), context)
        .expect("global Date object must be found");
    let date_obj = date.as_object().expect("global Date must be an object");
    date_obj.set(
        js_string!("now"),
        date_now_fn.to_js_function(context.realm()),
        false,
        context,
    )?;
    Ok(())
}

// Property key for storing join set index
const JOIN_SET_IDX_KEY: &str = "__joinSetIdx__";

/// Create a JoinSet wrapper object with methods.
fn create_join_set_object(js: JoinSet, ctx: &mut Context) -> JsResult<JsValue> {
    let obj = new_object(ctx);

    // Store the join set ID
    let js_id = js.id();

    // Store the join set and get its index
    let idx = store_join_set(js);

    // Store index on the object for methods to retrieve
    obj.set(
        js_string!(JOIN_SET_IDX_KEY),
        JsValue::from(idx as u32),
        false,
        ctx,
    )?;

    // Store the ID as a string property
    obj.set(js_string!("__id__"), js_string!(js_id), false, ctx)?;

    // joinSet.id()
    let id_fn = NativeFunction::from_fn_ptr(|this, _args, ctx| {
        let this_obj = this
            .as_object()
            .ok_or_else(|| JsNativeError::typ().with_message("this is not an object"))?;
        let id = this_obj.get(js_string!("__id__"), ctx)?;
        Ok(id)
    });
    obj.set(
        js_string!("id"),
        id_fn.to_js_function(ctx.realm()),
        false,
        ctx,
    )?;

    // joinSet.submit(ffqn, params, [config])
    let submit_fn = NativeFunction::from_fn_ptr(|this, args, ctx| {
        let this_obj = this
            .as_object()
            .ok_or_else(|| JsNativeError::typ().with_message("this is not an object"))?;
        let idx = this_obj
            .get(js_string!(JOIN_SET_IDX_KEY), ctx)?
            .to_u32(ctx)? as usize;

        let ffqn = args
            .get_or_undefined(0)
            .as_string()
            .ok_or_else(|| JsNativeError::typ().with_message("ffqn must be a string"))?
            .to_std_string_escaped();

        // Parse FFQN: "namespace:pkg/interface.function"
        let (ifc_name, fn_name) = parse_ffqn(&ffqn)?;
        let function = Function {
            interface_name: ifc_name,
            function_name: fn_name,
        };

        // Serialize params to JSON
        let params_val = args.get_or_undefined(1);
        let params_json = json_stringify(params_val, ctx)?;

        // Parse optional config
        let config = if let Some(cfg_val) = args.get(2) {
            parse_submit_config(cfg_val, ctx)?
        } else {
            None
        };

        let backtrace = capture_backtrace(ctx);
        let result = with_join_set(idx, |js| {
            submit_json_bt(js, &function, &params_json, config, Some(&backtrace))
        })?;

        match result {
            Ok(exec_id) => Ok(JsValue::from(js_string!(exec_id.id))),
            Err(e) => Err(JsNativeError::error()
                .with_message(format!("submit failed: {:?}", e))
                .into()),
        }
    });
    obj.set(
        js_string!("submit"),
        submit_fn.to_js_function(ctx.realm()),
        false,
        ctx,
    )?;

    // joinSet.submitDelay(schedule)
    let submit_delay_fn = NativeFunction::from_fn_ptr(|this, args, ctx| {
        let this_obj = this
            .as_object()
            .ok_or_else(|| JsNativeError::typ().with_message("this is not an object"))?;
        let idx = this_obj
            .get(js_string!(JOIN_SET_IDX_KEY), ctx)?
            .to_u32(ctx)? as usize;

        let schedule = parse_schedule_at(args.get_or_undefined(0), ctx)?;
        let backtrace = capture_backtrace(ctx);

        let delay_id = with_join_set(idx, |js| submit_delay_bt(js, schedule, Some(&backtrace)))?;

        Ok(JsValue::from(js_string!(delay_id.id)))
    });
    obj.set(
        js_string!("submitDelay"),
        submit_delay_fn.to_js_function(ctx.realm()),
        false,
        ctx,
    )?;

    // joinSet.joinNext()
    let join_next_fn = NativeFunction::from_fn_ptr(|this, _args, ctx| {
        let this_obj = this
            .as_object()
            .ok_or_else(|| JsNativeError::typ().with_message("this is not an object"))?;
        let idx = this_obj
            .get(js_string!(JOIN_SET_IDX_KEY), ctx)?
            .to_u32(ctx)? as usize;

        let backtrace = capture_backtrace(ctx);
        let join_result = with_join_set(idx, |js| join_next_bt(js, Some(&backtrace)))?;

        match join_result {
            Ok((response_id, result)) => {
                let result_obj = new_object(ctx);

                match response_id {
                    ResponseId::ExecutionId(exec_id) => {
                        result_obj.set(js_string!("type"), js_string!("execution"), false, ctx)?;
                        result_obj.set(js_string!("id"), js_string!(exec_id.id), false, ctx)?;
                        result_obj.set(js_string!("ok"), result.is_ok(), false, ctx)?;
                    }
                    ResponseId::DelayId(delay_id) => {
                        result_obj.set(js_string!("type"), js_string!("delay"), false, ctx)?;
                        result_obj.set(js_string!("id"), js_string!(delay_id.id), false, ctx)?;
                        result_obj.set(js_string!("ok"), result.is_ok(), false, ctx)?;
                    }
                }

                Ok(result_obj.into())
            }
            Err(_) => Err(JsNativeError::error()
                .with_message("JoinSetEmpty: all responses processed")
                .into()),
        }
    });
    obj.set(
        js_string!("joinNext"),
        join_next_fn.to_js_function(ctx.realm()),
        false,
        ctx,
    )?;

    // joinSet.joinNextTry()
    let join_next_try_fn = NativeFunction::from_fn_ptr(|this, _args, ctx| {
        let this_obj = this
            .as_object()
            .ok_or_else(|| JsNativeError::typ().with_message("this is not an object"))?;
        let idx = this_obj
            .get(js_string!(JOIN_SET_IDX_KEY), ctx)?
            .to_u32(ctx)? as usize;

        let backtrace = capture_backtrace(ctx);
        let join_result = with_join_set(idx, |js| join_next_try_bt(js, Some(&backtrace)))?;

        match join_result {
            Ok((response_id, result)) => {
                let result_obj = new_object(ctx);

                match response_id {
                    ResponseId::ExecutionId(exec_id) => {
                        result_obj.set(js_string!("type"), js_string!("execution"), false, ctx)?;
                        result_obj.set(js_string!("id"), js_string!(exec_id.id), false, ctx)?;
                        result_obj.set(js_string!("ok"), result.is_ok(), false, ctx)?;
                    }
                    ResponseId::DelayId(delay_id) => {
                        result_obj.set(js_string!("type"), js_string!("delay"), false, ctx)?;
                        result_obj.set(js_string!("id"), js_string!(delay_id.id), false, ctx)?;
                        match result {
                            Ok(()) => {
                                result_obj.set(js_string!("ok"), true, false, ctx)?;
                            }
                            Err(()) => {
                                result_obj.set(js_string!("ok"), false, false, ctx)?;
                                result_obj.set(
                                    js_string!("error"),
                                    js_string!("cancelled"),
                                    false,
                                    ctx,
                                )?;
                            }
                        }
                    }
                }

                Ok(result_obj.into())
            }
            Err(JoinNextTryError::AllProcessed) => {
                let result_obj = new_object(ctx);
                result_obj.set(js_string!("status"), js_string!("allProcessed"), false, ctx)?;
                Ok(result_obj.into())
            }
            Err(JoinNextTryError::Pending) => {
                let result_obj = new_object(ctx);
                result_obj.set(js_string!("status"), js_string!("pending"), false, ctx)?;
                Ok(result_obj.into())
            }
        }
    });
    obj.set(
        js_string!("joinNextTry"),
        join_next_try_fn.to_js_function(ctx.realm()),
        false,
        ctx,
    )?;

    // joinSet.close()
    let close_fn = NativeFunction::from_fn_ptr(|this, _args, ctx| {
        let this_obj = this
            .as_object()
            .ok_or_else(|| JsNativeError::typ().with_message("this is not an object"))?;
        let idx = this_obj
            .get(js_string!(JOIN_SET_IDX_KEY), ctx)?
            .to_u32(ctx)? as usize;

        if let Some(js) = take_join_set(idx) {
            let backtrace = capture_backtrace(ctx);
            join_set_close_bt(js, Some(&backtrace));
        }
        Ok(JsValue::undefined())
    });
    obj.set(
        js_string!("close"),
        close_fn.to_js_function(ctx.realm()),
        false,
        ctx,
    )?;

    Ok(obj.into())
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
