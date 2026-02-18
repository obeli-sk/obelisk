//! JavaScript runtime using Boa engine for Obelisk JS workflows.
//!
//! This runtime provides:
//! - `obelisk.*` workflow API (join sets, sleep, random, etc.)
//! - `console.*` → `obelisk:log` routing
//!
//! Unlike activity-js-runtime, this runtime:
//! - Does NOT provide `fetch()` (HTTP is not deterministic)
//! - Uses synchronous execution only (no async/promises)
//! - Runs on `wasm32-unknown-unknown` target

use crate::generated::exports::obelisk_workflow::workflow_js_runtime::execute::JsRuntimeError;
use crate::generated::obelisk::log::log as obelisk_log;
use crate::generated::obelisk::types::execution::{ExecutionId, Function, ResponseId};
use crate::generated::obelisk::types::time::{Datetime, Duration, ScheduleAt};
use crate::generated::obelisk::workflow::workflow_support::{
    JoinNextTryError, SubmitConfig, get_result_json, join_next, join_next_try, join_set_close,
    join_set_create, join_set_create_named, random_string, random_u64, random_u64_inclusive,
    schedule_json, sleep, submit_delay, submit_json,
};
use boa_engine::{
    Context, JsArgs, JsNativeError, JsObject, JsResult, JsValue, NativeFunction, Source, js_string,
    property::Attribute,
};
use std::cell::RefCell;

// Use the resource type from generated bindings
use crate::generated::obelisk::types::join_set::JoinSet;

// Thread-local storage for JoinSets (WASM is single-threaded)
thread_local! {
    static JOIN_SETS: RefCell<Vec<Option<JoinSet>>> = RefCell::new(Vec::new());
}

fn store_join_set(js: JoinSet) -> usize {
    JOIN_SETS.with(|sets| {
        let mut sets = sets.borrow_mut();
        let idx = sets.len();
        sets.push(Some(js));
        idx
    })
}

fn with_join_set<T, F: FnOnce(&JoinSet) -> T>(idx: usize, f: F) -> Option<T> {
    JOIN_SETS.with(|sets| {
        let sets = sets.borrow();
        sets.get(idx).and_then(|opt| opt.as_ref()).map(f)
    })
}

fn take_join_set(idx: usize) -> Option<JoinSet> {
    JOIN_SETS.with(|sets| {
        let mut sets = sets.borrow_mut();
        sets.get_mut(idx).and_then(|opt| opt.take())
    })
}

/// Execute JavaScript code with the given parameters.
///
/// `params_json` is a list of JSON-serialized parameter values.
/// Each element is passed as a positional argument to the JS function `fn_name`.
pub fn execute(
    fn_name: &str,
    js_code: &str,
    params_json: &[String],
) -> Result<Result<String, String>, JsRuntimeError> {
    // `fn_name` comes from trusted `workflow_js_worker`, must be FFQN's fn name
    let fn_name = fn_name.replace('-', "_");

    let mut context = Context::default();

    // Set up the obelisk global object with workflow APIs
    setup_obelisk_api(&mut context).expect("obelisk API setup must work");

    // Set up console
    setup_console(&mut context).expect("console setup must work");

    // `params_json` is sent by trusted `workflow_js_worker`, params were typechecked.
    // Parse each JSON param and store as `__params__` array.
    let params_js_parts: Vec<&str> = params_json.iter().map(|p| p.as_str()).collect();
    let params_array = format!("const __params__ = [{}];", params_js_parts.join(", "));
    context
        .eval(Source::from_bytes(&params_array))
        .expect("already verified that params_json elements are parseable");

    // Add the function to the context, without running it.
    let bare_fn_eval = context.eval(Source::from_bytes(js_code));
    if let Err(err) = bare_fn_eval {
        obelisk_log::error(&format!("cannot evaluate - {err:?}"));
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

    // Spread params as positional arguments: fn_name(__params__[0], __params__[1], ...)
    let spread_args: Vec<String> = (0..params_json.len())
        .map(|i| format!("__params__[{i}]"))
        .collect();
    let call_fn = format!("{fn_name}({});", spread_args.join(", "));
    let result = context.eval(Source::from_bytes(&call_fn));

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
        let string = string.to_std_string_escaped();
        return Some(string);
    }
    None
}

/// Helper to create a new JS object with the default prototype.
fn new_object(ctx: &mut Context) -> JsObject {
    JsObject::default(ctx.intrinsics())
}

/// Set up the global `obelisk` object with workflow support functions.
fn setup_obelisk_api(context: &mut Context) -> JsResult<()> {
    let obelisk = new_object(context);

    // obelisk.createJoinSet([options])
    let create_join_set = NativeFunction::from_fn_ptr(|_this, args, ctx| {
        // Check for options.name
        if let Some(options) = args.get(0) {
            if let Some(obj) = options.as_object() {
                if let Ok(name_val) = obj.get(js_string!("name"), ctx) {
                    if let Some(name) = name_val.as_string() {
                        let name_str = name.to_std_string_escaped();
                        return match join_set_create_named(&name_str) {
                            Ok(js) => Ok(create_join_set_object(js, ctx)?),
                            Err(e) => Err(JsNativeError::error()
                                .with_message(format!("Failed to create named join set: {:?}", e))
                                .into()),
                        };
                    }
                }
            }
        }

        let js = join_set_create();
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
        match sleep(schedule) {
            Ok(dt) => {
                let result = new_object(ctx);
                result.set(js_string!("seconds"), JsValue::from(dt.seconds), false, ctx)?;
                result.set(
                    js_string!("nanoseconds"),
                    JsValue::from(dt.nanoseconds),
                    false,
                    ctx,
                )?;
                Ok(result.into())
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
    let random_u64_fn = NativeFunction::from_fn_ptr(|_this, args, ctx| {
        let min = args.get_or_undefined(0).to_u32(ctx)? as u64;
        let max = args.get_or_undefined(1).to_u32(ctx)? as u64;
        let result = random_u64(min, max);
        Ok(JsValue::from(result))
    });
    obelisk.set(
        js_string!("randomU64"),
        random_u64_fn.to_js_function(context.realm()),
        false,
        context,
    )?;

    // obelisk.randomU64Inclusive(min, max)
    let random_u64_inclusive_fn = NativeFunction::from_fn_ptr(|_this, args, ctx| {
        let min = args.get_or_undefined(0).to_u32(ctx)? as u64;
        let max = args.get_or_undefined(1).to_u32(ctx)? as u64;
        let result = random_u64_inclusive(min, max);
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
        let result = random_string(min_len, max_len);
        Ok(JsValue::from(js_string!(result)))
    });
    obelisk.set(
        js_string!("randomString"),
        random_string_fn.to_js_function(context.realm()),
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

        match get_result_json(&exec_id) {
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

    // obelisk.schedule(ffqn, params, scheduleAt, config?)
    let schedule_fn = NativeFunction::from_fn_ptr(|_this, args, ctx| {
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

        let schedule = parse_schedule_at(args.get_or_undefined(2), ctx)?;

        let config = if let Some(cfg_val) = args.get(3) {
            parse_submit_config(cfg_val, ctx)?
        } else {
            None
        };

        match schedule_json(schedule, &function, &params_json, config) {
            Ok(exec_id) => Ok(JsValue::from(js_string!(exec_id.id))),
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

    // Set obelisk as global
    context.register_global_property(js_string!("obelisk"), obelisk, Attribute::all())?;

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

        let result = with_join_set(idx, |js| submit_json(js, &function, &params_json, config))
            .ok_or_else(|| JsNativeError::error().with_message("JoinSet has been closed"))?;

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

        let delay_id = with_join_set(idx, |js| submit_delay(js, schedule))
            .ok_or_else(|| JsNativeError::error().with_message("JoinSet has been closed"))?;

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

        let join_result = with_join_set(idx, |js| join_next(js))
            .ok_or_else(|| JsNativeError::error().with_message("JoinSet has been closed"))?;

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

        let join_result = with_join_set(idx, |js| join_next_try(js))
            .ok_or_else(|| JsNativeError::error().with_message("JoinSet has been closed"))?;

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
            join_set_close(js);
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
    if let Ok(ms) = obj.get(js_string!("milliseconds"), ctx) {
        if !ms.is_undefined() {
            let ms_val = ms.to_u32(ctx)? as u64;
            return Ok(ScheduleAt::In(Duration::Milliseconds(ms_val)));
        }
    }

    if let Ok(secs) = obj.get(js_string!("seconds"), ctx) {
        if !secs.is_undefined() {
            let secs_val = secs.to_u32(ctx)? as u64;
            return Ok(ScheduleAt::In(Duration::Seconds(secs_val)));
        }
    }

    if let Ok(mins) = obj.get(js_string!("minutes"), ctx) {
        if !mins.is_undefined() {
            let mins_val = mins.to_u32(ctx)?;
            return Ok(ScheduleAt::In(Duration::Minutes(mins_val)));
        }
    }

    if let Ok(hours) = obj.get(js_string!("hours"), ctx) {
        if !hours.is_undefined() {
            let hours_val = hours.to_u32(ctx)?;
            return Ok(ScheduleAt::In(Duration::Hours(hours_val)));
        }
    }

    if let Ok(days) = obj.get(js_string!("days"), ctx) {
        if !days.is_undefined() {
            let days_val = days.to_u32(ctx)?;
            return Ok(ScheduleAt::In(Duration::Days(days_val)));
        }
    }

    // Check for absolute time
    if let Ok(at) = obj.get(js_string!("at"), ctx) {
        if !at.is_undefined() {
            let at_obj = at.as_object().ok_or_else(|| {
                JsNativeError::typ()
                    .with_message("'at' must be an object with seconds and nanoseconds")
            })?;
            let seconds = at_obj.get(js_string!("seconds"), ctx)?.to_u32(ctx)? as u64;
            let nanoseconds = at_obj.get(js_string!("nanoseconds"), ctx)?.to_u32(ctx)?;
            return Ok(ScheduleAt::At(Datetime {
                seconds,
                nanoseconds,
            }));
        }
    }

    Ok(ScheduleAt::Now)
}

/// Parse FFQN string into interface name and function name.
fn parse_ffqn(ffqn: &str) -> JsResult<(String, String)> {
    // Format: "namespace:pkg/interface.function" or "namespace:pkg/interface@version.function"
    let dot_pos = ffqn.rfind('.').ok_or_else(|| {
        JsNativeError::error().with_message(format!(
            "Invalid FFQN '{}': missing function name separator '.'",
            ffqn
        ))
    })?;

    let interface_name = &ffqn[..dot_pos];
    let function_name = &ffqn[dot_pos + 1..];

    if function_name.is_empty() {
        return Err(JsNativeError::error()
            .with_message(format!("Invalid FFQN '{}': empty function name", ffqn))
            .into());
    }

    Ok((interface_name.to_string(), function_name.to_string()))
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

    if let Ok(ms) = obj.get(js_string!("milliseconds"), ctx) {
        if !ms.is_undefined() {
            return Ok(Duration::Milliseconds(ms.to_u32(ctx)? as u64));
        }
    }

    if let Ok(secs) = obj.get(js_string!("seconds"), ctx) {
        if !secs.is_undefined() {
            return Ok(Duration::Seconds(secs.to_u32(ctx)? as u64));
        }
    }

    if let Ok(mins) = obj.get(js_string!("minutes"), ctx) {
        if !mins.is_undefined() {
            return Ok(Duration::Minutes(mins.to_u32(ctx)?));
        }
    }

    if let Ok(hours) = obj.get(js_string!("hours"), ctx) {
        if !hours.is_undefined() {
            return Ok(Duration::Hours(hours.to_u32(ctx)?));
        }
    }

    if let Ok(days) = obj.get(js_string!("days"), ctx) {
        if !days.is_undefined() {
            return Ok(Duration::Days(days.to_u32(ctx)?));
        }
    }

    Err(JsNativeError::typ()
        .with_message("duration must have milliseconds, seconds, minutes, hours, or days")
        .into())
}

/// Convert JS value to JSON string.
fn json_stringify(value: &JsValue, ctx: &mut Context) -> JsResult<String> {
    // Use built-in JSON.stringify
    let json = ctx.global_object().get(js_string!("JSON"), ctx)?;
    let json_obj = json
        .as_object()
        .ok_or_else(|| JsNativeError::error().with_message("JSON global not found"))?;
    let stringify = json_obj.get(js_string!("stringify"), ctx)?;
    let stringify_fn = stringify
        .as_callable()
        .ok_or_else(|| JsNativeError::error().with_message("JSON.stringify not callable"))?;

    let result = stringify_fn.call(&json, &[value.clone()], ctx)?;

    result
        .as_string()
        .map(|s| s.to_std_string_escaped())
        .ok_or_else(|| {
            JsNativeError::error()
                .with_message("JSON.stringify returned non-string")
                .into()
        })
}

/// Set up the global `console` object routing to obelisk:log.
fn setup_console(context: &mut Context) -> JsResult<()> {
    let console = new_object(context);

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
                json_stringify(v, ctx)
                    .or_else(|_| v.to_string(ctx).map(|s| s.to_std_string_escaped()))
            }
        })
        .collect();

    Ok(parts?.join(" "))
}
