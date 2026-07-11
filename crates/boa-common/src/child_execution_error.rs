//! `obelisk.ChildExecutionError` — the single catchable type thrown when an
//! awaited child execution (or a cancelled delay) fails.
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
//! The native data ([`ChildExecutionError`]) is intentionally empty: every
//! user-visible field lives as an ordinary JS own property. The Rust struct
//! exists only as a brand that the producer can detect via `downcast_ref`,
//! rather than sniffing a spoofable `.name`/prototype.

use boa_engine::{
    Context, JsError, JsNativeError, JsObject, JsResult, JsValue, Source,
    class::{Class, ClassBuilder},
    js_string,
};
use boa_gc::{Finalize, Trace};

/// Native brand stored inside every `obelisk.ChildExecutionError` instance.
#[derive(Debug, Trace, Finalize, boa_engine::JsData)]
pub struct ChildExecutionError;

impl Class for ChildExecutionError {
    const NAME: &'static str = "ChildExecutionError";
    const LENGTH: usize = 1;

    fn data_constructor(
        _new_target: &JsValue,
        _args: &[JsValue],
        _context: &mut Context,
    ) -> JsResult<Self> {
        Ok(ChildExecutionError)
    }

    fn object_constructor(
        instance: &JsObject<Self>,
        args: &[JsValue],
        context: &mut Context,
    ) -> JsResult<()> {
        let obj = instance.clone().upcast();
        let message = args.first().cloned().unwrap_or(JsValue::undefined());
        obj.set(js_string!("message"), message, false, context)?;
        obj.set(
            js_string!("name"),
            js_string!("ChildExecutionError"),
            false,
            context,
        )?;
        Ok(())
    }

    fn init(_class: &mut ClassBuilder<'_>) -> JsResult<()> {
        Ok(())
    }
}

/// Register `obelisk.ChildExecutionError`.
///
/// Must run after the global `obelisk` object has been installed. Registers the
/// native class, moves the constructor under `obelisk` (off the global
/// namespace), and splices its prototype chain onto `Error` so that
/// `instanceof Error` holds and instances inherit `Error.prototype`.
pub fn register(context: &mut Context) -> JsResult<()> {
    context.register_global_class::<ChildExecutionError>()?;
    context.eval(Source::from_bytes(
        "obelisk.ChildExecutionError = globalThis.ChildExecutionError;\n\
         delete globalThis.ChildExecutionError;\n\
         Object.setPrototypeOf(obelisk.ChildExecutionError.prototype, Error.prototype);\n\
         Object.setPrototypeOf(obelisk.ChildExecutionError, Error);",
    ))?;
    Ok(())
}

/// Resolved pieces of a child-execution failure, gathered by each runtime from
/// its own host bindings before delegating message/object construction here.
pub struct ChildExecutionErrorParts<'a> {
    /// Completed child execution id; `None` for a delay.
    pub child_id: Option<&'a str>,
    /// Cancelled delay id; `Some` only for a cancelled delay.
    pub delay_id: Option<&'a str>,
    /// Business `err` payload as a JSON string; `None` for a unit err or a
    /// platform failure.
    pub value_json: Option<&'a str>,
    /// Execution-failure kind as a kebab string (mirrors the WIT
    /// `execution-failure-kind` enum) for a platform failure; `None` for a
    /// business err or a cancelled delay.
    pub failure_kind: Option<&'a str>,
    /// Whether the child or delay was cancelled.
    pub cancelled: bool,
}

/// Build a `ChildExecutionError` and wrap it as a throwable [`JsError`].
pub fn make_child_execution_error(
    parts: &ChildExecutionErrorParts,
    context: &mut Context,
) -> JsResult<JsError> {
    // `.value` is the parsed business payload, and `undefined` for a platform
    // failure (kind set, synthesized sentinel dropped) or a unit err.
    let value = match (parts.failure_kind, parts.value_json) {
        (Some(_), _) | (None, None) => JsValue::undefined(),
        (None, Some(json)) => context.eval(Source::from_bytes(&format!("({json})")))?,
    };

    let message = build_message(parts, &value);

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
fn build_message(parts: &ChildExecutionErrorParts, value: &JsValue) -> String {
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

/// Fetch the `obelisk.ChildExecutionError` constructor.
fn ctor(context: &mut Context) -> JsResult<JsObject> {
    let global = context.global_object();
    let obelisk = global
        .get(js_string!("obelisk"), context)?
        .as_object()
        .ok_or_else(|| JsNativeError::error().with_message("global obelisk object missing"))?;
    obelisk
        .get(js_string!("ChildExecutionError"), context)?
        .as_object()
        .ok_or_else(|| {
            JsNativeError::error()
                .with_message("obelisk.ChildExecutionError missing")
                .into()
        })
}
