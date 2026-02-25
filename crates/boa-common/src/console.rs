//! Console implementation for Boa JS engine routing to Obelisk logging.

use boa_engine::{
    Context, JsObject, JsResult, JsValue, NativeFunction, js_string, property::Attribute,
};

/// Logging functions required by the console setup.
pub trait ObeliskLogger: Copy + 'static {
    fn trace(&self, msg: &str);
    fn debug(&self, msg: &str);
    fn info(&self, msg: &str);
    fn warn(&self, msg: &str);
    fn error(&self, msg: &str);
}

/// Set up the global `console` object routing to the provided logger.
pub fn setup_console<L: ObeliskLogger>(context: &mut Context, logger: L) -> JsResult<()> {
    let console = JsObject::default(context.intrinsics());

    macro_rules! console_method {
        ($name:expr, $log_method:ident) => {{
            let func = NativeFunction::from_copy_closure(move |_this, args, ctx| {
                let msg = console_args_to_string(args, ctx)?;
                logger.$log_method(&msg);
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

    console_method!("trace", trace);
    console_method!("debug", debug);
    console_method!("log", info);
    console_method!("info", info);
    console_method!("warn", warn);
    console_method!("error", error);

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

/// Convert JS value to JSON string using built-in JSON.stringify.
pub fn json_stringify(value: &JsValue, ctx: &mut Context) -> JsResult<String> {
    use boa_engine::{JsNativeError, js_string};

    let json = ctx
        .global_object()
        .get(js_string!("JSON"), ctx)
        .expect("global JSON object must be found");
    let json_obj = json.as_object().expect("JSON global must be an object");
    let stringify = json_obj
        .get(js_string!("stringify"), ctx)
        .expect("stringify must exist on JSON object");
    let stringify_fn = stringify
        .as_callable()
        .expect("JSON.stringify must be callable");

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
