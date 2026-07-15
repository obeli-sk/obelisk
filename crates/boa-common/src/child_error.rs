//! `obelisk.ChildError`: the single catchable type thrown when an awaited
//! child execution, a cancelled delay, or a cancelled `obelisk.sleep` fails.
//!
//! It replaces the earlier "throw the raw err value / `throw null`" behavior:
//! - Always truthy and `instanceof Error` (its prototype is spliced onto
//!   `Error.prototype`), with a legible `message` and a JS stack.
//! - Carries the err payload on `.value` plus metadata (`.childId`,
//!   `.cancelled`, `.failureKind`).
//! - Re-propagates transparently: the producer runtime detects a re-thrown
//!   instance via the native brand (`downcast_ref`) and re-serializes `.value`,
//!   so `throw e` behaves like `throw e.value`.
//!
//! `obelisk.ChildExecutionError` (the 0.40.0 name) stays as a deprecated alias
//! for the same constructor, so `instanceof` checks against either name are
//! identical.
//!
//! The native data ([`ChildError`]) is intentionally empty: every user-visible
//! field lives as an ordinary JS own property. The Rust struct exists only as a
//! brand that the producer can detect via `downcast_ref`, rather than sniffing
//! a spoofable `.name`/prototype.

use boa_engine::{
    Context, JsError, JsNativeError, JsObject, JsResult, JsValue, Source,
    class::{Class, ClassBuilder},
    js_string,
};
use boa_gc::{Finalize, Trace};

/// Native brand stored inside every `obelisk.ChildError` instance.
#[derive(Debug, Trace, Finalize, boa_engine::JsData)]
pub struct ChildError;

impl Class for ChildError {
    const NAME: &'static str = "ChildError";
    const LENGTH: usize = 1;

    fn data_constructor(
        _new_target: &JsValue,
        _args: &[JsValue],
        _context: &mut Context,
    ) -> JsResult<Self> {
        Ok(ChildError)
    }

    fn object_constructor(
        instance: &JsObject<Self>,
        args: &[JsValue],
        context: &mut Context,
    ) -> JsResult<()> {
        let obj = instance.clone().upcast();
        let message = args.first().cloned().unwrap_or(JsValue::undefined());
        obj.set(js_string!("message"), message, false, context)?;
        obj.set(js_string!("name"), js_string!("ChildError"), false, context)?;
        Ok(())
    }

    fn init(_class: &mut ClassBuilder<'_>) -> JsResult<()> {
        Ok(())
    }
}

/// Register `obelisk.ChildError` (and its deprecated alias
/// `obelisk.ChildExecutionError`).
///
/// Must run after the global `obelisk` object has been installed. Registers the
/// native class, moves the constructor under `obelisk` (off the global
/// namespace), and splices its prototype chain onto `Error` so that
/// `instanceof Error` holds and instances inherit `Error.prototype`.
pub fn register(context: &mut Context) -> JsResult<()> {
    context.register_global_class::<ChildError>()?;
    // backcompat: 0.40.x apps catch obelisk.ChildExecutionError; keep the alias
    // (same constructor, so `instanceof` is identical) until the old name is removed.
    context.eval(Source::from_bytes(
        "obelisk.ChildError = globalThis.ChildError;\n\
         delete globalThis.ChildError;\n\
         Object.setPrototypeOf(obelisk.ChildError.prototype, Error.prototype);\n\
         Object.setPrototypeOf(obelisk.ChildError, Error);\n\
         obelisk.ChildExecutionError = obelisk.ChildError;",
    ))?;
    Ok(())
}

/// Resolved pieces of a child-execution failure, gathered by each runtime from
/// its own host bindings before delegating message/object construction here.
pub struct ChildErrorParts<'a> {
    /// Completed child execution id; `None` for a delay or a cancelled sleep.
    pub child_id: Option<&'a str>,
    /// Cancelled delay id; `Some` only for a cancelled delay.
    pub delay_id: Option<&'a str>,
    /// Business `err` payload as a JSON string; `None` for a unit err or a
    /// platform failure.
    pub value_json: Option<&'a str>,
    /// Execution-failure kind as a kebab string (mirrors the WIT
    /// `execution-failure-kind` enum) for a platform failure or a cancelled
    /// delay/sleep (`"cancelled"`); `None` for a business err.
    pub failure_kind: Option<&'a str>,
    /// Whether the child or delay was cancelled.
    pub cancelled: bool,
    /// Verbatim `.message` override; when `None` the message is derived from
    /// the other parts.
    pub message: Option<&'a str>,
}

/// Build a `ChildError` and wrap it as a throwable [`JsError`].
pub fn make_child_error(parts: &ChildErrorParts, context: &mut Context) -> JsResult<JsError> {
    // `.value` is the parsed business payload, and `undefined` for a platform
    // failure (kind set, synthesized sentinel dropped) or a unit err.
    let value = match (parts.failure_kind, parts.value_json) {
        (Some(_), _) | (None, None) => JsValue::undefined(),
        (None, Some(json)) => context.eval(Source::from_bytes(&format!("({json})")))?,
    };

    let message = match parts.message {
        Some(message) => message.to_string(),
        None => build_message(parts, &value),
    };

    // Construct via the registered constructor so the instance carries the
    // native brand and the (Error-spliced) prototype.
    let ctor = ctor(context)?;
    let instance = ctor.construct(
        &[JsValue::from(js_string!(message.as_str()))],
        None,
        context,
    )?;

    instance.set(js_string!("value"), value, false, context)?;
    instance.set(
        js_string!("childId"),
        parts
            .child_id
            .map_or(JsValue::undefined(), |id| js_string!(id).into()),
        false,
        context,
    )?;
    instance.set(
        js_string!("cancelled"),
        JsValue::from(parts.cancelled),
        false,
        context,
    )?;
    instance.set(
        js_string!("failureKind"),
        parts
            .failure_kind
            .map_or(JsValue::undefined(), |k| js_string!(k).into()),
        false,
        context,
    )?;
    // Best-effort JS stack captured at the throw site.
    if let Ok(stack) = context.eval(Source::from_bytes("new Error().stack")) {
        instance.set(js_string!("stack"), stack, false, context)?;
    }

    Ok(JsError::from_opaque(instance.into()))
}

/// A human-facing one-liner; programmatic access always goes through `.value`.
fn build_message(parts: &ChildErrorParts, value: &JsValue) -> String {
    if let Some(delay_id) = parts.delay_id {
        return format!("delay {delay_id} cancelled");
    }
    let mut msg = match parts.child_id {
        Some(id) => format!("child execution {id} failed"),
        None => "child execution failed".to_string(),
    };
    if let Some(kind) = parts.failure_kind {
        msg.push_str(&format!(": {kind}"));
    } else if let Some(s) = value.as_string() {
        msg.push_str(&format!(": {}", s.to_std_string_escaped()));
    }
    msg
}

/// Fetch the `obelisk.ChildError` constructor.
fn ctor(context: &mut Context) -> JsResult<JsObject> {
    let global = context.global_object();
    let obelisk = global
        .get(js_string!("obelisk"), context)?
        .as_object()
        .ok_or_else(|| JsNativeError::error().with_message("global obelisk object missing"))?;
    obelisk
        .get(js_string!("ChildError"), context)?
        .as_object()
        .ok_or_else(|| {
            JsNativeError::error()
                .with_message("obelisk.ChildError missing")
                .into()
        })
}
