#[allow(warnings)]
mod bindings;

use crate::bindings::exports::wasi::http::incoming_handler::Guest;
use bindings::wasi::http::types::{
    Fields, IncomingRequest, OutgoingBody, OutgoingResponse, ResponseOutparam,
};

struct Component;

bindings::export!(Component with_types_in bindings);

impl Guest for Component {
    fn handle(_request: IncomingRequest, outparam: ResponseOutparam) {
        let resp_headers = Fields::new();
        let resp = OutgoingResponse::new(resp_headers);
        let body = resp.body().expect("outgoing response");
        let n = std::env::var("N")
            .expect("env var `N` must be set")
            .parse()
            .expect("parameter `N` must be of type u8");

        let iterations = std::env::var("ITERATIONS")
            .expect("env var `ITERATIONS` must be set")
            .parse()
            .expect("parameter `ITERATIONS` must be of type u32"); // Panic here means we send 200 anyway.

        let fibo_res = if n >= 10 {
            // Submit new execution, do not wait for the result.
            let join_set_id = crate::bindings::obelisk::workflow::host_activities::new_join_set();
            let execution_id = bindings::testing::fibo_workflow_obelisk_ext::workflow::fiboa_submit(
                &join_set_id,
                n,
                iterations,
            );
            format!("submitted: {execution_id}")
        } else if n >= 5 {
            // Submit new execution, wait for the result.
            let join_set_id = crate::bindings::obelisk::workflow::host_activities::new_join_set();
            bindings::testing::fibo_workflow_obelisk_ext::workflow::fiboa_submit(
                &join_set_id,
                n,
                iterations,
            );
            let fibo_res = bindings::testing::fibo_workflow_obelisk_ext::workflow::fiboa_await_next(
                &join_set_id,
            );
            format!("submit/await-next: {fibo_res}")
        } else if n > 1 {
            // Call the execution directly.
            let fibo_res = bindings::testing::fibo_workflow::workflow::fiboa(n, iterations);
            format!("direct call: {fibo_res}")
        } else {
            "hardcoded: 1".to_string() // For performance testing - no activity is called
        };
        // Start sending the 200 OK response
        ResponseOutparam::set(outparam, Ok(resp));
        let out = body.write().expect("outgoing stream");
        out.blocking_write_and_flush(format!("fiboa({n}, {iterations}) = {fibo_res}").as_bytes())
            .expect("writing response");
        drop(out);
        OutgoingBody::finish(body, None).unwrap();
    }
}
