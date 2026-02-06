use assert_matches::assert_matches;
use generated::export;
use generated::exports::testing::fibo_workflow::workflow::Guest;
use generated::obelisk::types::execution::Function;
use generated::obelisk::workflow::workflow_support::{
    GetResultJsonError, SubmitJsonError, get_result_json, join_set_create, submit_json,
};
use generated::testing::{
    fibo::fibo::fibo as fibo_activity,
    fibo_obelisk_ext::fibo::{fibo_await_next, fibo_submit},
};

use crate::generated::obelisk::types::execution::ResponseId;

mod generated {
    #![allow(clippy::all)]
    include!(concat!(env!("OUT_DIR"), "/any.rs"));
}

struct Component;
export!(Component with_types_in generated);

pub fn black_box<T>(dummy: T) -> T {
    unsafe {
        let ret = std::ptr::read_volatile(&raw const dummy);
        std::mem::forget(dummy);
        ret
    }
}

impl Guest for Component {
    fn fibow(n: u8, iterations: u32) -> Result<u64, ()> {
        let mut last = 0;
        for _ in 0..iterations {
            last = fibo(black_box(n));
        }
        Ok(last)
    }

    fn fiboa(n: u8, iterations: u32) -> Result<u64, ()> {
        let mut last = 0;
        for _ in 0..iterations {
            last = fibo_activity(n).unwrap();
        }
        Ok(last)
    }

    fn fiboa_concurrent(n: u8, iterations: u32) -> Result<u64, ()> {
        let join_set = join_set_create();
        for _ in 0..iterations {
            fibo_submit(&join_set, n);
        }
        let mut last = 0;
        for _ in 0..iterations {
            last = fibo_await_next(&join_set).unwrap().1.unwrap();
        }
        Ok(last)
    }

    fn fiboa_submit_json(n: u8) -> Result<u64, ()> {
        // Create a join set
        let join_set = join_set_create();

        // Submit using JSON params - the fibo function takes a single u8 parameter
        let params_json = format!("[{n}]");
        let function = Function {
            interface_name: "testing:fibo/fibo".to_string(),
            function_name: "fibo".to_string(),
        };

        let execution_id = submit_json(&join_set, &function, &params_json, None)
            .expect("submit_json should succeed");

        // Await the result
        let (response_id, is_success) = join_set.join_next().expect("all-processed cannot happen");
        let result_exec_id = assert_matches!(response_id, ResponseId::ExecutionId(exe) => exe);
        // Verify the execution IDs match
        assert_eq!(
            execution_id.id, result_exec_id.id,
            "execution IDs should match"
        );
        assert!(is_success.is_ok());

        // Now get the result using get_result_json
        let json_result = get_result_json(&execution_id).expect("get_result_json should succeed");

        // json_result is Result<Option<String>, Option<String>>
        // For fibo, we expect Ok(Some(json_string)) containing the u64 value
        match json_result {
            Ok(Some(json_str)) => {
                // Parse the JSON string to get the u64 value
                let value: u64 = serde_json::from_str(&json_str).expect("should be valid u64 JSON");
                Ok(value)
            }
            Ok(None) => panic!("expected Some value, got None"),
            Err(_) => Err(()),
        }
    }

    /// Test submit-json with unknown FFQN → FunctionNotFound error.
    fn test_submit_json_unknown_ffqn() -> Result<(), String> {
        let join_set = join_set_create();
        let function = Function {
            interface_name: "testing:nonexistent/ifc".to_string(),
            function_name: "unknown-fn".to_string(),
        };
        match submit_json(&join_set, &function, "[]", None) {
            Err(SubmitJsonError::FunctionNotFound) => Ok(()),
            Err(other) => Err(format!("expected FunctionNotFound, got {other:?}")),
            Ok(_) => Err("expected error, got Ok".to_string()),
        }
    }

    /// Test submit-json with malformed JSON params → ParamsParsingError.
    fn test_submit_json_malformed_params() -> Result<(), String> {
        let join_set = join_set_create();
        let function = Function {
            interface_name: "testing:fibo/fibo".to_string(),
            function_name: "fibo".to_string(),
        };

        // Test 1: Invalid JSON syntax
        match submit_json(&join_set, &function, "not valid json", None) {
            Err(SubmitJsonError::ParamsParsingError(msg)) => {
                if !msg.contains("cannot parse params as JSON array") {
                    return Err(format!("unexpected error message: {msg}"));
                }
            }
            Err(other) => return Err(format!("expected ParamsParsingError, got {other:?}")),
            Ok(_) => return Err("expected error, got Ok".to_string()),
        }

        // Test 2: Valid JSON but not an array
        match submit_json(&join_set, &function, "42", None) {
            Err(SubmitJsonError::ParamsParsingError(msg)) => {
                if !msg.contains("params must be a json array") {
                    return Err(format!("unexpected error message: {msg}"));
                }
            }
            Err(other) => return Err(format!("expected ParamsParsingError, got {other:?}")),
            Ok(_) => return Err("expected error, got Ok".to_string()),
        }

        // Test 3: Valid JSON array but wrong type
        match submit_json(&join_set, &function, r#"["not a number"]"#, None) {
            Err(SubmitJsonError::ParamsParsingError(msg)) => {
                if !msg.contains("type checking failed") {
                    return Err(format!("unexpected error message: {msg}"));
                }
            }
            Err(other) => return Err(format!("expected ParamsParsingError, got {other:?}")),
            Ok(_) => return Err("expected error, got Ok".to_string()),
        }

        Ok(())
    }

    /// Test get-result-json before await → NotFoundInProcessedResponses.
    fn test_get_result_json_before_await() -> Result<(), String> {
        let join_set = join_set_create();
        let function = Function {
            interface_name: "testing:fibo/fibo".to_string(),
            function_name: "fibo".to_string(),
        };

        // Submit an execution
        let execution_id = submit_json(&join_set, &function, "[10]", None)
            .map_err(|e| format!("submit_json failed: {e:?}"))?;

        // Try to get result before awaiting - should fail
        match get_result_json(&execution_id) {
            Err(GetResultJsonError::NotFoundInProcessedResponses) => Ok(()),
            Err(other) => Err(format!(
                "expected NotFoundInProcessedResponses, got {other:?}"
            )),
            Ok(_) => Err("expected error, got Ok".to_string()),
        }
    }

    /// Test get-result-json when activity returns error variant → Err(None) since fibo returns result<u64>.
    fn test_get_result_json_err_variant() -> Result<(), String> {
        let join_set = join_set_create();
        let function = Function {
            interface_name: "testing:fibo/fibo".to_string(),
            function_name: "fibo".to_string(),
        };

        // Submit with n=50 which causes fibo activity to return Err(())
        let execution_id = submit_json(&join_set, &function, "[50]", None)
            .map_err(|e| format!("submit_json failed: {e:?}"))?;

        // Await the result using the typed extension function
        let (result_exec_id, result) =
            fibo_await_next(&join_set).map_err(|e| format!("fibo_await_next failed: {e:?}"))?;

        // Verify the execution IDs match
        if execution_id.id != result_exec_id.id {
            return Err(format!(
                "execution IDs don't match: {} vs {}",
                execution_id.id, result_exec_id.id
            ));
        }

        // Verify the typed result is Err
        if result.is_ok() {
            return Err("expected activity to return error, got Ok".to_string());
        }

        // Now get the result using get_result_json
        let json_result =
            get_result_json(&execution_id).map_err(|e| format!("get_result_json failed: {e:?}"))?;

        // json_result should be Err(None) since fibo returns result<u64> (error type is unit)
        match json_result {
            Err(None) => Ok(()), // Unit error type serializes to null -> None
            Err(Some(json)) => Err(format!("expected Err(None), got Err(Some({json}))")),
            Ok(v) => Err(format!("expected Err, got Ok({v:?})")),
        }
    }
}

#[must_use]
pub fn fibo(n: u8) -> u64 {
    if n == 0 {
        0
    } else if n == 1 {
        1
    } else {
        fibo(n - 1) + fibo(n - 2)
    }
}
