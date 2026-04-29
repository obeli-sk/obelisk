//! Shared utilities for JS workers (activity-js and workflow-js).
//!
//! Both workers wrap a Boa WASM component that returns `result<string, string>` where
//! the inner strings are JSON-encoded values. These helpers handle the common logic for
//! mapping JSON-encoded ok/err values to user-typed return values.

use concepts::{
    ResultParsingError, ResultParsingErrorFromVal, ReturnTypeExtendable,
    SupportedFunctionReturnValue, storage::Version,
};
use executor::worker::{FatalError, WorkerError};

/// Maps a JSON-encoded JS ok return value to the user-configured ok type.
pub(crate) fn map_js_ok_to_user_retval(
    ok_val: Option<&serde_json::Value>,
    user_return_type: &ReturnTypeExtendable,
    version: Version,
) -> Result<SupportedFunctionReturnValue, WorkerError> {
    match (&user_return_type.type_wrapper_tl.ok, ok_val) {
        (Some(configured_ok_type), Some(ok_val)) => {
            let wvt =
                val_json::wast_val_ser::deserialize_value(ok_val, *configured_ok_type.clone())
                    .map_err(|err| {
                        WorkerError::FatalError(
                            FatalError::ResultParsingError(
                                ResultParsingError::ResultParsingErrorFromVal(
                                    ResultParsingErrorFromVal::TypeCheckError(format!(
                                        "failed to type check the return value `{ok_val}` as type {configured_ok_type} - {err}"
                                    )),
                                ),
                            ),
                            version.clone(),
                        )
                    })?;
            Ok(SupportedFunctionReturnValue::Ok(Some(wvt)))
        }
        (None, _) => Ok(SupportedFunctionReturnValue::Ok(None)), // Convenience: unit type accepts (blocks) any response.
        (Some(ty), ok_val) => Err(WorkerError::FatalError(
            FatalError::ResultParsingError(ResultParsingError::ResultParsingErrorFromVal(
                ResultParsingErrorFromVal::TypeCheckError(format!(
                    "return value type check failed, expected value of type {ty}, got {ok_val}",
                    ok_val = ok_val
                        .map(|ok_val| format!("`{ok_val}`"))
                        .unwrap_or_else(|| "empty response".to_string())
                )),
            )),
            version,
        )),
    }
}

/// Maps a JSON-encoded JS throw to the user-configured err type.
///
/// The Boa runtime JSON-encodes all thrown values (consistent with ok values), so
/// `throw null` → `"null"`, `throw "foo"` → `"\"foo\""`, `throw "my-case"` → `"\"my-case\""`.
///
/// * `err: None` (void) — only JSON null is accepted → `Err(None)`; anything else is fatal.
/// * `err: Some(T)` — the JSON is type-checked and deserialized via `deserialize_value`.
pub(crate) fn map_js_throw_to_user_err(
    thrown: &str,
    user_return_type: &ReturnTypeExtendable,
    version: Version,
) -> Result<SupportedFunctionReturnValue, WorkerError> {
    let thrown_val: serde_json::Value =
        serde_json::from_str(thrown).unwrap_or(serde_json::Value::Null);
    match user_return_type.type_wrapper_tl.err.as_deref() {
        None => {
            if thrown_val == serde_json::Value::Null {
                Ok(SupportedFunctionReturnValue::Err(None))
            } else {
                let declared_return_type = user_return_type.to_string();
                Err(WorkerError::FatalError(
                    FatalError::ResultParsingError(ResultParsingError::ResultParsingErrorFromVal(
                        ResultParsingErrorFromVal::TypeCheckError(format!(
                            "thrown value type check failed: return type is `{declared_return_type}` (no error \
                             type), expected `throw null`, got `{thrown}`"
                        )),
                    )),
                    version,
                ))
            }
        }
        Some(user_err_type) => {
            let wvt = val_json::wast_val_ser::deserialize_value(&thrown_val, user_err_type.clone())
                .map_err(|e| {
                    WorkerError::FatalError(
                        FatalError::ResultParsingError(
                            ResultParsingError::ResultParsingErrorFromVal(
                                ResultParsingErrorFromVal::TypeCheckError(format!(
                                    "failed to type check thrown value `{thrown}` as \
                                     `{user_err_type}`: {e}"
                                )),
                            ),
                        ),
                        version.clone(),
                    )
                })?;
            Ok(SupportedFunctionReturnValue::Err(Some(wvt)))
        }
    }
}
