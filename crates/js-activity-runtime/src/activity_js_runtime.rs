//! JavaScript runtime using Boa engine for Obelisk JS activities.
//!
//! This is a simplified runtime without workflow-specific APIs
//! (no join sets, submit, sleep, etc.). It provides:
//! - `console.*` → `obelisk:log` routing
//! - `main(arg0, arg1, ...)` invocation with spread params

use boa_engine::{
    Context, JsNativeError, JsObject, JsResult, JsValue, NativeFunction, Source, js_string,
    property::Attribute,
};

use crate::generated::obelisk::log::log as obelisk_log;

/// Execute JavaScript code with the given parameters.
///
/// `params_json` is expected to be a JSON array string. The elements
/// are spread as positional arguments to the `main` function.
pub fn execute(js_code: &str, params_json: &str) -> Result<String, String> {
    let mut context = Context::default();

    // Set up console
    setup_console(&mut context).map_err(|e| format!("Failed to setup console: {e}"))?;

    // Verify that params is a valid JSON array
    let parsed: serde_json::Value = serde_json::from_str(params_json)
        .map_err(|err| format!("params '{params_json}' must be valid JSON: {err}"))?;
    if !parsed.is_array() {
        return Err(format!("params must be a JSON array, got: {parsed}"));
    }

    // Parse params and store as global __params__ array
    let escaped_params = params_json.replace('\\', "\\\\").replace('\'', "\\'");
    let params_code = format!("var __params__ = JSON.parse('{escaped_params}');");
    context
        .eval(Source::from_bytes(&params_code))
        .map_err(|e| format!("Failed to parse params '{params_json}': {e}"))?;

    // Build full code: stringify helper + user code + main invocation with spread
    const JS_PRE: &str = r#"
    function __stringify__(e) {
        if (e === null) return "null";
        if (e === undefined) return "undefined";
        if (typeof e === "string") return e;
        if (typeof e === "number" || typeof e === "boolean") return JSON.stringify(e);

        if (e instanceof Error) {
            return JSON.stringify({
                type: "Error",
                name: e.name,
                message: e.message,
                stack: e.stack
            });
        }

        try {
            return JSON.stringify(e);
        } catch {
            return JSON.stringify({
                type: typeof e,
                value: String(e),
                note: "Unserializable (circular)"
            });
        }
    }
    "#;
    const JS_POST: &str = r#"
    if (typeof main !== 'function') {
        throw 'main function not defined';
    }
    try {
        const __result__ = main.apply(null, __params__);
        __stringify__(__result__);
    } catch (e) {
        throw __stringify__(e);
    }
    "#;

    let full_code = format!("{JS_PRE}\n{js_code}\n{JS_POST}");

    let result = context.eval(Source::from_bytes(&full_code)).map_err(|e| {
        if let Some(js_value) = e.as_opaque() {
            js_value
                .as_string()
                .map(|s| s.to_std_string_escaped())
                .unwrap_or_else(|| "Error is not a string".to_string())
        } else {
            e.to_string()
        }
    })?;

    result
        .as_string()
        .map(|s| s.to_std_string_escaped())
        .ok_or_else(|| format!("result is not a string: {result:?}"))
}

/// Helper to create a new JS object with the default prototype.
fn new_object(ctx: &mut Context) -> JsObject {
    JsObject::default(ctx.intrinsics())
}

/// Convert JS value to JSON string using the built-in JSON.stringify.
fn json_stringify(value: &JsValue, ctx: &mut Context) -> JsResult<String> {
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
