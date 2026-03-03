//! Common helper utilities for Boa JS runtimes.

use boa_engine::{Context, JsNativeError, JsObject, JsResult};

/// Helper to create a new JS object with the default prototype.
pub fn new_object(ctx: &mut Context) -> JsObject {
    JsObject::default(ctx.intrinsics())
}

/// Parse FFQN string into interface name and function name.
///
/// Format: "namespace:pkg/interface.function" or "namespace:pkg/interface@version.function"
///
/// Returns `(interface_name, function_name)`.
pub fn parse_ffqn(ffqn: &str) -> JsResult<(String, String)> {
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

/// Extract a string message from a `JsError`.
///
/// Handles the case where `throw 'string'` is used (opaque error with string value).
pub fn extract_error_string(err: &boa_engine::JsError) -> Option<String> {
    if let Some(js_value) = err.as_opaque()
        && let Some(string) = js_value.as_string()
    {
        let string = string.to_std_string_escaped();
        return Some(string);
    }
    None
}
