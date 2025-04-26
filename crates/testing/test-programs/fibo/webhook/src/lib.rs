use crate::obelisk::log::log::info;
use crate::obelisk::types::time::ScheduleAt;
use crate::testing::fibo_workflow::workflow;
use crate::testing::fibo_workflow_obelisk_ext::workflow as workflow_ext;
use waki::{ErrorCode, Request, Response, handler};
use wit_bindgen::generate;

generate!({ generate_all });

#[handler]
fn handle(_req: Request) -> Result<Response, ErrorCode> {
    let Ok(n) = std::env::var("N")
        .expect("env var `N` must be set by Obelisk if routes are configured properly")
        .parse()
    else {
        return Response::builder().status_code(400).build();
    };

    let Ok(iterations) = std::env::var("ITERATIONS")
        .expect("env var `ITERATIONS` must be set by Obelisk if routes are configured properly")
        .parse()
    else {
        return Response::builder().status_code(400).build();
    };

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
    Response::builder()
        .body(format!("fiboa({n}, {iterations}) = {fibo_res}"))
        .build()
}
