use crate::generated::obelisk::log::log as obelisk_log;
use boa_engine::{
    Context, JsNativeError, JsObject, JsResult, JsValue, NativeFunction, js_string,
    property::Attribute,
};

/// Set up the global `console` object routing to obelisk:log.
pub fn setup_console(context: &mut Context) -> JsResult<()> {
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
