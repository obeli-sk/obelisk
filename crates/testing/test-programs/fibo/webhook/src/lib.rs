use crate::generated::obelisk::log::log::info;
use crate::generated::obelisk::types::time::ScheduleAt;
use crate::generated::testing::fibo_workflow::workflow;
use crate::generated::testing::fibo_workflow_obelisk_schedule::workflow as workflow_schedule;
use wstd::http::body::Body;
use wstd::http::{Error, Request, Response, StatusCode};

mod generated {
    #![allow(clippy::empty_line_after_outer_attr)]
    include!(concat!(env!("OUT_DIR"), "/any.rs"));
}

#[wstd::http_server]
async fn main(_request: Request<Body>) -> Result<Response<Body>, Error> {
    let Ok(n) = std::env::var("N")
        .expect("env var `N` must be set by Obelisk if routes are configured properly")
        .parse()
    else {
        return Ok(Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body(Body::empty())?);
    };

    let Ok(iterations) = std::env::var("ITERATIONS")
        .expect("env var `ITERATIONS` must be set by Obelisk if routes are configured properly")
        .parse()
    else {
        return Ok(Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body(Body::empty())?);
    };

    let fibo_res = if n >= 10 {
        println!("scheduling");
        let execution_id = workflow_schedule::fiboa_schedule(ScheduleAt::Now, n, iterations);
        format!("scheduled: {}", execution_id.id)
    } else if n > 1 {
        // Call the execution directly.
        println!("direct call");
        let fibo_res = workflow::fiboa(n, iterations).unwrap();
        format!("direct call: {fibo_res}")
    } else {
        assert_eq!(iterations, 0); // For testing traps
        println!("hardcoded");
        "hardcoded: 1".to_string() // For performance testing - no activity is called
    };
    info(&format!("Sending response {fibo_res}"));
    Ok(Response::builder().body(Body::from(format!("fiboa({n}, {iterations}) = {fibo_res}")))?)
}
