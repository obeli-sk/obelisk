#[allow(warnings)]
mod bindings;

use bindings::wasi::http::types::{
    Fields, IncomingRequest, OutgoingBody, OutgoingResponse, ResponseOutparam,
};

use crate::bindings::exports::wasi::http::incoming_handler::Guest;

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
        // Start sending the 200 OK response
        ResponseOutparam::set(outparam, Ok(resp));
        let iterations = std::env::var("ITERATIONS")
            .expect("env var `ITERATIONS` must be set")
            .parse()
            .expect("parameter `ITERATIONS` must be of type u32"); // Panic here means we send 200 anyway.
        let out = body.write().expect("outgoing stream");
        let fibo_res = if n >= 10 {
            // Submit new execution, do not wait for result
            let join_set_id = crate::bindings::obelisk::workflow::host_activities::new_join_set();
            let execution_id = bindings::testing::fibo_workflow_obelisk_ext::workflow::fiboa_submit(
                &join_set_id,
                n,
                iterations,
            );
            format!("calculating in the background, see {execution_id}")
        } else if n > 1 {
            // Call the execution directly.
            bindings::testing::fibo_workflow::workflow::fiboa(n, iterations).to_string()
        } else {
            "1".to_string() // For performance testing - no activity is called
        };
        out.blocking_write_and_flush(format!("fiboa({n}, {iterations}) = {fibo_res}").as_bytes())
            .expect("writing response");
        drop(out);
        OutgoingBody::finish(body, None).unwrap();
    }
}
