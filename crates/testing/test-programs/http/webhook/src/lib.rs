use crate::generated::obelisk::types::time::ScheduleAt;
use crate::generated::testing::fibo_workflow_obelisk_schedule::workflow as workflow_schedule;
use wstd::http::body::Body;
use wstd::http::{Client, Error, Method, Request, Response, StatusCode};

mod generated {
    #![allow(clippy::all)]
    include!(concat!(env!("OUT_DIR"), "/any.rs"));
}

#[wstd::http_server]
async fn main(_request: Request<Body>) -> Result<Response<Body>, Error> {
    let port = std::env::var("PORT").expect("env var `PORT` must be set");
    let url = format!("http://127.0.0.1:{port}/");

    // Make an outgoing HTTP GET request
    let outgoing_req = Request::builder()
        .method(Method::GET)
        .uri(&url)
        .body(Body::empty())
        .unwrap();
    let resp = Client::new().send(outgoing_req).await.unwrap();
    let status = resp.status().as_u16();
    let mut resp_body = resp.into_body();
    let body_bytes = resp_body.contents().await.unwrap();
    let body_str = String::from_utf8_lossy(body_bytes).into_owned();

    // Schedule an execution to trigger get_version_or_create
    let execution_id = workflow_schedule::fiboa_schedule(ScheduleAt::Now, 10, 1);

    // Return body and scheduled execution id separated by newline
    let response_text = format!("{body_str}\n{}", execution_id.id);
    Ok(Response::builder()
        .status(StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR))
        .body(Body::from(response_text))?)
}
