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
//! ```js
//! const currentId = obelisk.executionIdCurrent();
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
//! // Wait for next result (blocks until a child or delay completes)
//! const result = js.joinNext();
//! // returns the child ok value, null for a completed delay, or throws on err
//! // js.lastId is also set to the completed child or delay id
//!
//! // Non-blocking variant:
//! const tryResponse = js.joinNextTry();
//! // undefined if no child finished yet
//! // otherwise returns the child ok value, or throws on err
//! // throws obelisk.JoinSetExhaustedError when the join set is exhausted
//!
//! // Get the actual result value (returns ok value, throws err value)
//! const result = obelisk.getResult(execId);
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
//! // A workflow submits a stub activity execution, obtaining its execution id.
//! const js = obelisk.createJoinSet();
//! const execId = js.submit("ns:pkg/activity.stub_fn", [args]);
//! // The same or another workflow can provide the result using an import:
//! import { fooStub } from "ns:pkg-obelisk-stub/activity.stub_fn";
//! fooStub(execId, result)
//! // or dynamically:
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
//! const namedWakeUp = obelisk.sleep({ seconds: 30 }, "retry-timeout"); // Date
//! console.log(wakeUp.toISOString()); // e.g. "2024-01-01T00:00:00.500Z"
//! // A cancelled sleep throws an obelisk.ChildError with
//! // cancelled=true and failureKind="cancelled".
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
//! const result = js.joinNext();
//! if (result === null && js.lastId === delayId) {
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
use crate::generated::exports::obelisk_workflow::workflow_js_runtime::execute::{
    JsRuntimeError, ResolvedInterfaceImports,
};
use crate::generated::obelisk::log::log as obelisk_log;
use crate::generated::obelisk::types::backtrace::{FrameInfo, FrameSymbol, WasmBacktrace};
use crate::generated::obelisk::types::execution::{
    ExecutionFailureKind, ExecutionId, Function, ResponseId,
};
use crate::generated::obelisk::types::join_set::JoinSet;
use crate::generated::obelisk::types::time::{Datetime, Duration, ScheduleAt};
use crate::generated::obelisk::workflow::workflow_support::{
    self, JoinNextTryError, get_execution_failure_kind, get_result_json, last_direct_call_id,
};
use crate::generated::obelisk::workflow::workflow_support_backtrace::{
    call_json, execution_id_generate, join_next, join_next_try, join_set_close, join_set_create,
    join_set_create_named, random_string, random_u64, random_u64_inclusive, schedule_json, sleep,
    stub_json, submit_delay, submit_json,
};
use boa_common::child_error::{ChildError, ChildErrorParts, make_child_error};
use boa_common::console::{ObeliskLogger, json_stringify, setup_console};
use boa_common::helpers::{new_object, parse_ffqn};
use boa_common::imports::{self, ProxyKind};
use boa_engine::context::time::{Clock, JsInstant};
use boa_engine::module::MapModuleLoader;
use boa_engine::{
    Context, JsArgs, JsError, JsNativeError, JsResult, JsString, JsValue, Module, NativeFunction,
    Source,
    builtins::promise::PromiseState,
    js_string,
    object::builtins::{JsDate, JsFunction},
    property::{Attribute, PropertyDescriptor},
};
use std::cell::RefCell;
use std::collections::HashMap;
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

/// Create a `NativeFunction` that proxies a direct call to `call-json-bt`.
///
/// When invoked from JS, the function:
/// 1. JSON-serializes all arguments into a JSON array string
/// 2. Calls the host `call_json` with the derived FFQN
/// 3. Returns the Ok value or throws the Err value
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
            // Serialize args as JSON array: [arg0, arg1, ...]
            let array = boa_engine::object::builtins::JsArray::new(ctx)?;
            for (i, arg) in args.iter().enumerate() {
                array.set(i as u32, arg.clone(), false, ctx)?;
            }
            let params_json = json_stringify(&array.into(), ctx)?;

            let backtrace = capture_backtrace(ctx);
            match call_json(&function, &params_json, Some(&backtrace)) {
                Ok(Ok(Some(json_str))) => ctx.eval(Source::from_bytes(&format!("({})", json_str))),
                Ok(Ok(None)) => Ok(JsValue::null()),
                Ok(Err(payload)) => {
                    let child = last_direct_call_id();
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
/// `execution-id-generate` + `schedule-json` from `obelisk:workflow/workflow-support`.
///
/// JS signature: `myFuncSchedule(scheduleAt, ...args) → executionId`
fn create_schedule_proxy(
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

            // First argument is scheduleAt
            let schedule = parse_schedule_at(args.get_or_undefined(0), ctx)?;

            // Remaining arguments are the function params
            let array = boa_engine::object::builtins::JsArray::new(ctx)?;
            for (i, arg) in args.iter().skip(1).enumerate() {
                array.set(i as u32, arg.clone(), false, ctx)?;
            }
            let params_json = json_stringify(&array.into(), ctx)?;

            let backtrace = capture_backtrace(ctx);
            let exec_id = execution_id_generate(Some(&backtrace));

            match schedule_json(
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
        (ifc, func),
    );
    native.to_js_function(context.realm()).into()
}

/// Create a `NativeFunction` that proxies `submit-json-bt` for extension imports.
///
/// JS signature: `addSubmit(joinSet, ...args) → executionId`
fn create_ext_submit_proxy(
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

            // First arg is the join set object
            let js_obj = args.get_or_undefined(0).as_object().ok_or_else(|| {
                JsNativeError::typ().with_message("first argument must be a join set")
            })?;
            let idx = js_obj.get(js_string!(JOIN_SET_IDX_KEY), ctx)?.to_u32(ctx)? as usize;

            // Remaining args are function params
            let array = boa_engine::object::builtins::JsArray::new(ctx)?;
            for (i, arg) in args.iter().skip(1).enumerate() {
                array.set(i as u32, arg.clone(), false, ctx)?;
            }
            let params_json = json_stringify(&array.into(), ctx)?;

            let backtrace = capture_backtrace(ctx);
            let result = with_join_set(idx, |js| {
                submit_json(js, &function, &params_json, Some(&backtrace))
            })?;

            match result {
                Ok(exec_id) => Ok(JsValue::from(js_string!(exec_id.id))),
                Err(e) => Err(JsNativeError::error()
                    .with_message(format!("submit failed: {:?}", e))
                    .into()),
            }
        },
        (ifc, func),
    );
    native.to_js_function(context.realm()).into()
}

/// Create a `NativeFunction` that proxies `join-next-bt` + `get-result-json-bt`
/// for extension imports.
///
/// JS signature: `addAwaitNext(joinSet) → okValue` (throws err value)
///
/// The execution ID of the child that completed is stored on the join set
/// object as `lastId` (readable via `js.lastId`).
fn create_ext_await_next_proxy(context: &mut Context) -> JsValue {
    let native = NativeFunction::from_fn_ptr(|_this, args, ctx| {
        let js_obj = args.get_or_undefined(0).as_object().ok_or_else(|| {
            JsNativeError::typ().with_message("first argument must be a join set")
        })?;
        let idx = js_obj.get(js_string!(JOIN_SET_IDX_KEY), ctx)?.to_u32(ctx)? as usize;

        let backtrace = capture_backtrace(ctx);
        // `join-next` now returns the value directly; the id is read via `last-id`
        // (exposed to JS through the `lastId` accessor on the join set object).
        let (join_result, last_id) = with_join_set(idx, |js| {
            let result = join_next(js, Some(&backtrace));
            (result, js.last_id())
        })?;

        match join_result {
            Ok(inner_result) => match last_id {
                Some(ResponseId::ExecutionId(exec_id)) => {
                    unwrap_result(inner_result, exec_id.id.as_str(), ctx)
                }
                Some(ResponseId::DelayId(_)) => Err(JsNativeError::error()
                    .with_message("unexpected delay response in awaitNext")
                    .into()),
                None => Err(JsNativeError::error()
                    .with_message("missing last-id after join-next")
                    .into()),
            },
            Err(_) => Err(JsNativeError::error()
                .with_message("JoinSetEmpty: all responses processed")
                .into()),
        }
    });
    native.to_js_function(context.realm()).into()
}

/// Create a `NativeFunction` that proxies `get-result-json-bt` for extension imports.
///
/// JS signature: `addGet(execId) → okValue` (throws on err)
fn create_ext_get_proxy(context: &mut Context) -> JsValue {
    let native = NativeFunction::from_fn_ptr(|_this, args, ctx| {
        let exec_id_str = args
            .get_or_undefined(0)
            .as_string()
            .ok_or_else(|| JsNativeError::typ().with_message("executionId must be a string"))?
            .to_std_string_escaped();

        let exec_id = ExecutionId {
            id: exec_id_str.clone(),
        };

        match get_result_json(&exec_id) {
            Ok(inner_result) => unwrap_result(inner_result, &exec_id_str, ctx),
            Err(e) => Err(JsNativeError::error()
                .with_message(format!("get result failed: {:?}", e))
                .into()),
        }
    });
    native.to_js_function(context.realm()).into()
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
        ExecutionFailureKind::Uncategorized => "uncategorized",
    }
}

/// Build a `ChildError` for a failed child execution, disambiguating a
/// business `err` from a platform failure via `get-execution-failure-kind`.
fn child_error(exec_id: &str, payload: Option<String>, ctx: &mut Context) -> JsResult<JsError> {
    let exec = ExecutionId {
        id: exec_id.to_string(),
    };
    match get_execution_failure_kind(&exec) {
        Ok(Some(kind)) => {
            let cancelled = matches!(kind, ExecutionFailureKind::Cancelled);
            make_child_error(
                &ChildErrorParts {
                    child_id: Some(exec_id),
                    delay_id: None,
                    value_json: None,
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
        // Failure kind is unresolvable (e.g. not-yet-processed): surface as a
        // plain host error rather than a ChildError.
        Err(e) => Ok(JsNativeError::error()
            .with_message(format!("failed to resolve child failure kind: {:?}", e))
            .into()),
    }
}

/// Build a `ChildError` for a failed direct call (`obelisk.call` / an
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

/// If `js_err` is a re-thrown [`obelisk.ChildError`], return its `.value`
/// (the original business payload). Detection is brand-safe via `downcast_ref` on
/// the native marker, not by sniffing `.name`/prototype.
fn child_error_rethrow_value(js_err: &JsError, ctx: &mut Context) -> Option<JsValue> {
    let obj = js_err.as_opaque()?.as_object()?;
    if obj.downcast_ref::<ChildError>().is_some() {
        obj.get(js_string!("value"), ctx).ok()
    } else {
        None
    }
}

/// Build a `ChildError` for a cancelled delay.
fn delay_cancelled_error(delay_id: &str, ctx: &mut Context) -> JsResult<JsError> {
    make_child_error(
        &ChildErrorParts {
            child_id: None,
            delay_id: Some(delay_id),
            value_json: None,
            failure_kind: Some("cancelled"),
            cancelled: true,
            message: None,
        },
        ctx,
    )
}

fn new_join_set_exhausted_error(ctx: &mut Context) -> JsError {
    let message = "JoinSetEmpty: all responses processed";
    let fallback = || JsNativeError::error().with_message(message).into();

    let err = (|| -> JsResult<JsError> {
        let global = ctx.global_object();
        let obelisk = global
            .get(js_string!("obelisk"), ctx)?
            .as_object()
            .ok_or_else(|| JsNativeError::error().with_message("global obelisk object missing"))?;
        let ctor = obelisk
            .get(js_string!("JoinSetExhaustedError"), ctx)?
            .as_object()
            .ok_or_else(|| {
                JsNativeError::error().with_message("obelisk.JoinSetExhaustedError missing")
            })?;
        let err_obj = ctor.construct(&[JsValue::from(js_string!(message))], None, ctx)?;
        Ok(JsError::from_opaque(err_obj.into()))
    })();

    match err {
        Ok(err) => err,
        Err(_) => fallback(),
    }
}

/// Create a `NativeFunction` that proxies `stub-json` for stub imports.
///
/// JS signature: `fooStub(execId, result) → undefined`
///
/// The `result` argument is a JS value representing the execution result,
/// JSON-serialized before passing to `stub-json`.
fn create_stub_proxy(interface_name: &str, function_name: &str, context: &mut Context) -> JsValue {
    // interface_name and function_name are not used by stub_json itself,
    // but kept for consistency and future error messages.
    let _ifc = js_string!(interface_name);
    let _func = js_string!(function_name);

    let native = NativeFunction::from_fn_ptr(|_this, args, ctx| {
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
    native.to_js_function(context.realm()).into()
}

/// Workflow proxy factory for [`imports::register_import_modules`].
///
/// Routes to the appropriate host function proxy based on [`ProxyKind`].
fn create_workflow_proxy(kind: ProxyKind, context: &mut Context) -> JsValue {
    match kind {
        ProxyKind::DirectCall {
            interface_name,
            function_name,
        } => create_direct_call_proxy(interface_name, function_name, context),
        ProxyKind::Schedule {
            interface_name,
            function_name,
        } => create_schedule_proxy(interface_name, function_name, context),
        ProxyKind::ExtSubmit {
            interface_name,
            function_name,
        } => create_ext_submit_proxy(interface_name, function_name, context),
        ProxyKind::ExtAwaitNext => create_ext_await_next_proxy(context),
        ProxyKind::ExtGet => create_ext_get_proxy(context),
        ProxyKind::Stub {
            interface_name,
            function_name,
        } => create_stub_proxy(interface_name, function_name, context),
    }
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
    resolved_imports: Vec<ResolvedInterfaceImports>,
) -> Result<Result<String, String>, JsRuntimeError> {
    if let Some(js_file_name) = js_file_name {
        JS_FILE_NAME.with(|s| *s.borrow_mut() = js_file_name);
    }

    // Flatten typed records into the (specifier → [(js_name, wit_name)]) map
    // that `register_import_modules` expects.
    let imports: HashMap<String, Vec<(String, String)>> = resolved_imports
        .into_iter()
        .map(|ifc| {
            (
                ifc.ifc_fqn,
                ifc.functions
                    .into_iter()
                    .map(|f| (f.js_name, f.wit_name))
                    .collect(),
            )
        })
        .collect();
    let executor = Rc::new(DeterministicJobExecutor::default());
    let loader = Rc::new(MapModuleLoader::new());
    let mut context = Context::builder()
        .job_executor(executor.clone())
        .clock(Rc::new(ObeliskClock))
        .module_loader(loader.clone())
        .build()
        .expect("building context must work");

    // Set up the obelisk global object with workflow APIs BEFORE module evaluation
    // so that `obelisk.*` is available during module initialization
    setup_obelisk_api(&mut context).expect("obelisk API setup must work");

    // Set up console
    setup_console(&mut context, Logger).expect("console setup must work");

    // Override Math.random() to use deterministic random source
    setup_math_random(&mut context).expect("Math.random setup must work");

    // Register synthetic modules for WIT-style imports (e.g., 'ns:pkg/ifc').
    // Each imported function becomes a NativeFunction proxy to the appropriate
    // workflow host function (call-json-bt for direct, schedule-json for schedule).
    if !imports.is_empty() {
        imports::register_import_modules(&imports, &loader, &mut context, &create_workflow_proxy);
    }

    // Get the default export function from the ES module
    let default_fn = match get_default_export_workflow(js_code, &mut context) {
        Ok(func) => func,
        Err(err) => {
            let msg = match err {
                EsmErrorWorkflow::ParseError(msg) => format!("module parse error: {msg}"),
                EsmErrorWorkflow::LoadError(msg) => format!("module load error: {msg}"),
                EsmErrorWorkflow::LinkError(msg) => format!("module link error: {msg}"),
                EsmErrorWorkflow::EvalError(msg) => format!("module eval error: {msg}"),
                EsmErrorWorkflow::NoDefaultExport => "no default export".to_string(),
                EsmErrorWorkflow::DefaultNotCallable => {
                    "default export is not callable".to_string()
                }
            };
            obelisk_log::error(&msg);
            return Err(JsRuntimeError::CannotInstantiate(msg));
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
            // A re-thrown `ChildError` (`throw e`) is transparent: it
            // re-serializes its `.value`, so it behaves like `throw e.value`.
            // `throw new Error("msg")` → JSON-encode the message string.
            // `throw expr` (null, string, number, object, …) → serialize via to_json.
            let err_json = if let Some(value) = child_error_rethrow_value(&js_err, &mut context) {
                match value.to_json(&mut context) {
                    Ok(Some(json_val)) => serde_json::to_string(&json_val)
                        .expect("serde_json::Value must be serializable"),
                    Ok(None) => "null".to_string(), // undefined → null
                    Err(e) => {
                        return Err(JsRuntimeError::WrongThrownType(format!(
                            "cannot serialize re-thrown ChildError value to JSON: {e:?}"
                        )));
                    }
                }
            } else if let Ok(native_err) = js_err.try_native(&mut context) {
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

    // obelisk.executionIdCurrent()
    let current_execution_id_fn = NativeFunction::from_fn_ptr(|_this, _args, _ctx| {
        let exec_id = workflow_support::execution_id_current();
        Ok(JsValue::from(js_string!(exec_id.id)))
    });
    obelisk.set(
        js_string!("executionIdCurrent"),
        current_execution_id_fn.to_js_function(context.realm()),
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
            return match join_set_create_named(&name_str, Some(&backtrace)) {
                Ok(js) => Ok(create_join_set_object(js, ctx)?),
                Err(e) => Err(JsNativeError::error()
                    .with_message(format!("Failed to create named join set: {:?}", e))
                    .into()),
            };
        }

        let backtrace = capture_backtrace(ctx);
        let js = join_set_create(Some(&backtrace));
        create_join_set_object(js, ctx)
    });
    obelisk.set(
        js_string!("createJoinSet"),
        create_join_set.to_js_function(context.realm()),
        false,
        context,
    )?;

    // obelisk.sleep(schedule, [name])
    let sleep_fn = NativeFunction::from_fn_ptr(|_this, args, ctx| {
        let schedule = parse_schedule_at(args.get_or_undefined(0), ctx)?;
        let name = match args.get(1) {
            Some(arg) if !arg.is_null() && !arg.is_undefined() => Some(
                arg.as_string()
                    .ok_or_else(|| {
                        JsNativeError::typ().with_message("sleep name must be a string")
                    })?
                    .to_std_string_escaped(),
            ),
            _ => None,
        };
        let backtrace = capture_backtrace(ctx);
        match sleep(schedule, name.as_deref(), Some(&backtrace)) {
            Ok(dt) => {
                let ms = (dt.seconds as f64) * 1000.0 + (dt.nanoseconds as f64) / 1_000_000.0;
                // Construct `new Date(ms)` directly. Passing the timestamp avoids the
                // clock read that `JsDate::new` triggers via `ObeliskClock`, which would
                // append a spurious extra delay on every `obelisk.sleep`.
                let date_ctor = ctx.intrinsics().constructors().date().constructor();
                let date = date_ctor.construct(&[JsValue::from(ms)], Some(&date_ctor), ctx)?;
                Ok(date.into())
            }
            // A cancelled sleep throws a ChildError so it is catchable like any
            // other cancellation, with the message kept stable.
            Err(()) => Err(make_child_error(
                &ChildErrorParts {
                    child_id: None,
                    delay_id: None,
                    value_json: None,
                    failure_kind: Some("cancelled"),
                    cancelled: true,
                    message: Some("Sleep was cancelled"),
                },
                ctx,
            )?),
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
        let result = random_u64(min, max, Some(&backtrace));
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
        let result = random_u64_inclusive(min, max, Some(&backtrace));
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
        let result = random_string(min_len, max_len, Some(&backtrace));
        Ok(JsValue::from(js_string!(result)))
    });
    obelisk.set(
        js_string!("randomString"),
        random_string_fn.to_js_function(context.realm()),
        false,
        context,
    )?;

    // obelisk.call(ffqn, params)
    // Convenience: createJoinSet → submit → joinNext → getResult → close, return ok value, throw err value
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

        let backtrace = capture_backtrace(ctx);
        match call_json(&function, &params_json, Some(&backtrace)) {
            Ok(Ok(Some(json_str))) => {
                let parsed = ctx.eval(Source::from_bytes(&format!("({})", json_str)))?;
                Ok(parsed)
            }
            Ok(Ok(None)) => Ok(JsValue::null()),
            Ok(Err(payload)) => {
                let child = last_direct_call_id();
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

    // obelisk.getResult(executionId) — returns ok value, throws err value
    let get_result_fn = NativeFunction::from_fn_ptr(|_this, args, ctx| {
        let exec_id_str = args
            .get_or_undefined(0)
            .as_string()
            .ok_or_else(|| JsNativeError::typ().with_message("executionId must be a string"))?
            .to_std_string_escaped();

        let exec_id = ExecutionId {
            id: exec_id_str.clone(),
        };

        match get_result_json(&exec_id) {
            Ok(inner_result) => unwrap_result(inner_result, &exec_id_str, ctx),
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

    // obelisk.schedule(executionId, ffqn, params, scheduleAt?)
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

        let backtrace = capture_backtrace(ctx);
        match schedule_json(
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
    context.eval(Source::from_bytes(
        "obelisk.JoinSetExhaustedError = class JoinSetExhaustedError extends Error {\n\
            constructor(message) {\n\
                super(message);\n\
                this.name = 'JoinSetExhaustedError';\n\
                this.code = 'OBELISK_JOIN_SET_EXHAUSTED';\n\
            }\n\
        };",
    ))?;

    // obelisk.ChildError (plus the deprecated obelisk.ChildExecutionError alias):
    // native, brand-safe error thrown for a failed awaited child execution, a
    // cancelled delay, or a cancelled sleep.
    boa_common::child_error::register(context)?;

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
        let val = random_u64(0, max, Some(&backtrace));
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

/// Backs every wall-clock `Date` read (`new Date()`, `Date()`, `Date.now()`) with the
/// deterministic Obelisk clock via `sleep(Now)`, so time is stable across replay. Boa's
/// `Date` builtin reads `system_time_millis()` for all three paths.
struct ObeliskClock;

impl Clock for ObeliskClock {
    fn now(&self) -> JsInstant {
        // The monotonic clock is only consumed by timeout jobs (`setTimeout`), which
        // `DeterministicJobExecutor` already rejects. Reaching here would mean a
        // non-deterministic time source slipped into a workflow, so fail loudly.
        obelisk_log::error("Workflow must be deterministic, monotonic clock is not supported");
        panic!("monotonic clock is not supported in workflows")
    }

    fn system_time_millis(&self) -> i64 {
        let dt =
            sleep(ScheduleAt::Now, None, None).expect("clock read via sleep(Now) must not fail");
        (dt.seconds as i64) * 1000 + (dt.nanoseconds as i64) / 1_000_000
    }
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

    // joinSet.lastId — accessor over the WIT `join-set.last-id()`, the single
    // source of truth for the id of the most recently processed response. Returns
    // the execution/delay id string, or `undefined` if nothing has been processed
    // yet or the join set has been closed. `last-id` is a non-logged deterministic
    // in-memory read, so an accessor is replay-safe.
    let last_id_getter = NativeFunction::from_fn_ptr(|this, _args, ctx| {
        let this_obj = this
            .as_object()
            .ok_or_else(|| JsNativeError::typ().with_message("this is not an object"))?;
        let idx = this_obj
            .get(js_string!(JOIN_SET_IDX_KEY), ctx)?
            .to_u32(ctx)? as usize;
        match with_join_set(idx, |js| js.last_id()) {
            Ok(Some(ResponseId::ExecutionId(exec_id))) => Ok(js_string!(exec_id.id).into()),
            Ok(Some(ResponseId::DelayId(delay_id))) => Ok(js_string!(delay_id.id).into()),
            Ok(None) | Err(_) => Ok(JsValue::undefined()),
        }
    });
    obj.define_property_or_throw(
        js_string!("lastId"),
        PropertyDescriptor::builder()
            .maybe_get(Some(last_id_getter.to_js_function(ctx.realm())))
            .enumerable(true)
            .configurable(true),
        ctx,
    )?;

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

    // joinSet.submit(ffqn, params)
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

        let backtrace = capture_backtrace(ctx);
        let result = with_join_set(idx, |js| {
            submit_json(js, &function, &params_json, Some(&backtrace))
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

        let delay_id = with_join_set(idx, |js| submit_delay(js, schedule, Some(&backtrace)))?;

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
        // `join-next` returns the value directly; id/kind come from `last-id`.
        let (join_result, last_id) = with_join_set(idx, |js| {
            let result = join_next(js, Some(&backtrace));
            (result, js.last_id())
        })?;

        match join_result {
            Ok(inner_result) => match last_id {
                Some(ResponseId::ExecutionId(exec_id)) => {
                    unwrap_result(inner_result, exec_id.id.as_str(), ctx)
                }
                Some(ResponseId::DelayId(delay_id)) => match inner_result {
                    Ok(_) => Ok(JsValue::null()),
                    Err(_) => Err(delay_cancelled_error(delay_id.id.as_str(), ctx)?),
                },
                None => Err(JsNativeError::error()
                    .with_message("missing last-id after join-next")
                    .into()),
            },
            Err(_) => Err(new_join_set_exhausted_error(ctx)),
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
        let (join_result, last_id) = with_join_set(idx, |js| {
            let result = join_next_try(js, Some(&backtrace));
            (result, js.last_id())
        })?;

        match join_result {
            Ok(inner_result) => match last_id {
                Some(ResponseId::ExecutionId(exec_id)) => {
                    unwrap_result(inner_result, exec_id.id.as_str(), ctx)
                }
                Some(ResponseId::DelayId(delay_id)) => match inner_result {
                    Ok(_) => Ok(JsValue::null()),
                    Err(_) => Err(delay_cancelled_error(delay_id.id.as_str(), ctx)?),
                },
                None => Err(JsNativeError::error()
                    .with_message("missing last-id after join-next-try")
                    .into()),
            },
            Err(JoinNextTryError::AllProcessed) => Err(new_join_set_exhausted_error(ctx)),
            Err(JoinNextTryError::Pending) => Ok(JsValue::undefined()),
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
            join_set_close(js, Some(&backtrace));
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

    Err(JsNativeError::typ()
        .with_message(
            "schedule object has no recognized key (milliseconds, seconds, minutes, hours, days, at) and is not a Date",
        )
        .into())
}
