use generated::export;
use generated::exports::testing::fibo_workflow::workflow::Guest;
use generated::obelisk::types::execution::Function;
use generated::obelisk::workflow::workflow_support::{
    get_result_json, join_set_create, submit_json,
};
use generated::testing::{
    fibo::fibo::fibo as fibo_activity,
    fibo_obelisk_ext::fibo::{fibo_await_next, fibo_submit},
};

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

        // Await the result using the typed extension function
        // fibo_await_next returns (ExecutionId, Result<u64, ()>)
        let (result_exec_id, result) = fibo_await_next(&join_set).expect("await should succeed");

        // Verify the execution IDs match
        assert_eq!(
            execution_id.id, result_exec_id.id,
            "execution IDs should match"
        );

        // Now get the result using get_result_json
        let json_result = get_result_json(&execution_id).expect("get_result_json should succeed");

        // json_result is Result<Option<String>, Option<String>>
        // For fibo, we expect Ok(Some(json_string)) containing the u64 value
        match json_result {
            Ok(Some(json_str)) => {
                // Parse the JSON string to get the u64 value
                let value: u64 = serde_json::from_str(&json_str).expect("should be valid u64 JSON");
                // Verify it matches the typed result
                let typed_result = result.expect("typed result should be Ok");
                assert_eq!(value, typed_result, "JSON result should match typed result");
                Ok(value)
            }
            Ok(None) => panic!("expected Some value, got None"),
            Err(_) => Err(()),
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
