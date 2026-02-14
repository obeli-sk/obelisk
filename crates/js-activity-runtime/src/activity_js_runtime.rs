//! JavaScript runtime using Boa engine for Obelisk JS activities.
//!
//! This is a simplified runtime without workflow-specific APIs
//! It provides:
//! - `console.*` → `obelisk:log` routing

use boa_engine::{
    Context, JsNativeError, JsObject, JsResult, JsValue, NativeFunction, Source, js_string,
    property::Attribute,
};

use crate::generated::obelisk::log::log as obelisk_log;

/// Execute JavaScript code with the given parameters.
///
/// `params_json` is expected to be a JSON array string. The array is passed
/// as the first and only argument to `fn_name`.
pub fn execute(fn_name: &str, js_code: &str, params_json: &str) -> Result<String, String> {
    // `fn_name` comes from trusted `js_activity_worker`, must be FFQN's fn name
    let fn_name = fn_name.replace('-', "_");
    let mut context = Context::default();

    // Set up console
    setup_console(&mut context).map_err(|e| format!("Failed to setup console: {e}"))?;

    // `params_json` is sent by trusted `activity_js_worker`, params were typechecked.
    // Store as global `__params__` array.
    // Direct interpolation,  JSON array/object literals are valid JavaScript syntax.
    let params_code = format!("const __params__ = {params_json};");
    context
        .eval(Source::from_bytes(&params_code))
        .expect("already verified that params_json is parseable");

    // Add the function to the context, without running it.
    context
        .eval(Source::from_bytes(&js_code))
        .expect("TODO: permanent error - cannot evaluate the function");

    let typeof_fn = context.eval(Source::from_bytes(&format!("typeof {fn_name}")));
    let Ok(typeof_fn) = typeof_fn else { todo!() }; // permanent: function not defined
    let Some(typeof_fn) = typeof_fn.as_string() else {
        todo!() // permanent: typeof failed
    };
    if typeof_fn.as_str() != "function" {
        todo!("must be a function: {typeof_fn:?}") // // permanent: must be a function
    }

    let call_fn = format!("{fn_name}(__params__);");

    let result = context.eval(Source::from_bytes(&call_fn));

    match result {
        Ok(js_value) => {
            if let Some(string) = js_value.as_string() {
                Ok(string.to_std_string_escaped())
            } else {
                todo!("type check error: returned value must be string, got..");
            }
        }
        Err(js_err) => {
            if let Some(js_value) = js_err.as_opaque()
                && let Some(string) = js_value.as_string()
            {
                Err(string.to_std_string_escaped())
            } else {
                todo!("type check error: thrown value must be string, got..");
            }
        }
    }
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
