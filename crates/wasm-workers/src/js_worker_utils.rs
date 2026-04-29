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
use val_json::type_wrapper::TypeWrapper;

pub(crate) fn map_ok_variant(
    val: Option<serde_json::Value>,
    user_return_type: &ReturnTypeExtendable,
    version: Version,
) -> Result<SupportedFunctionReturnValue, WorkerError> {
    map_variant(
        val,
        version,
        user_return_type.type_wrapper_tl.ok.as_deref(),
        Ok(()),
    )
}
pub(crate) fn map_err_variant(
    val: Option<serde_json::Value>,
    user_return_type: &ReturnTypeExtendable,
    version: Version,
) -> Result<SupportedFunctionReturnValue, WorkerError> {
    map_variant(
        val,
        version,
        user_return_type.type_wrapper_tl.err.as_deref(),
        Err(()),
    )
}

fn map_variant(
    val: Option<serde_json::Value>,
    version: Version,
    expected_type: Option<&TypeWrapper>,
    variant: Result<(), ()>,
) -> Result<SupportedFunctionReturnValue, WorkerError> {
    let supported_func = if variant.is_ok() {
        SupportedFunctionReturnValue::Ok
    } else {
        SupportedFunctionReturnValue::Err
    };

    match (expected_type, val) {
        (Some(ty), Some(ok_val)) => {
            let wvt =
                val_json::wast_val_ser::deserialize_value(&ok_val, ty.clone())
                    .map_err(|err| {
                        WorkerError::FatalError(
                            FatalError::ResultParsingError(
                                ResultParsingError::ResultParsingErrorFromVal(
                                    ResultParsingErrorFromVal::TypeCheckError(format!(
                                        "failed to type check the {variant} variant value `{ok_val}` as type {ty} - {err}",
                                        variant = if variant.is_ok() { "ok" } else { "err" }
                                    )),
                                ),
                            ),
                            version.clone(),
                        )
                    })?;
            Ok(supported_func(Some(wvt)))
        }
        (None, _) => Ok(supported_func(None)), // Convenience: unit type accepts (blocks) any response.
        (Some(ty), val) => Err(WorkerError::FatalError(
            FatalError::ResultParsingError(ResultParsingError::ResultParsingErrorFromVal(
                ResultParsingErrorFromVal::TypeCheckError(format!(
                    "failed to type check the {variant} variant `{val} as type {ty}",
                    variant = if variant.is_ok() { "ok" } else { "err" },
                    val = val
                        .map(|val| format!("`{val}`"))
                        .unwrap_or_else(|| "empty response".to_string())
                )),
            )),
            version,
        )),
    }
}
