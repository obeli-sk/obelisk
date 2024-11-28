use crate::exports::wasi::http::incoming_handler::Guest;
use crate::obelisk::log::log::info;
use crate::obelisk::types::time::ScheduleAt;
use crate::testing::fibo_workflow::workflow;
use crate::testing::fibo_workflow_obelisk_ext::workflow as workflow_ext;
use crate::wasi::http::types::Fields;
use crate::wasi::http::types::IncomingRequest;
use crate::wasi::http::types::OutgoingBody;
use crate::wasi::http::types::OutgoingResponse;
use crate::wasi::http::types::ResponseOutparam;
use wit_bindgen::generate;

generate!({ generate_all });
struct Component;
export!(Component);

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
            println!("scheduling");
            let execution_id = workflow_ext::fiboa_schedule(ScheduleAt::Now, n, iterations);
            format!("scheduled: {}", execution_id.id)
        } else if n > 1 {
            // Call the execution directly.
            println!("direct call");
            let fibo_res = workflow::fiboa(n, iterations);
            format!("direct call: {fibo_res}")
        } else {
            println!("hardcoded");
            "hardcoded: 1".to_string() // For performance testing - no activity is called
        };
        info(&format!("Sending response {fibo_res}"));
        // Start sending the 200 OK response
        ResponseOutparam::set(outparam, Ok(resp));
        let out = body.write().expect("outgoing stream");
        out.blocking_write_and_flush(format!("fiboa({n}, {iterations}) = {fibo_res}").as_bytes())
            .expect("writing response");
        drop(out);
        OutgoingBody::finish(body, None).unwrap();
    }
}
