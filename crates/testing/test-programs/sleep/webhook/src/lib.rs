use anyhow::{Result, anyhow};
use generated::testing::sleep_workflow::workflow;
use wstd::http::body::Body;
use wstd::http::{Error, Request, Response, StatusCode};

mod generated {
    #![allow(clippy::all)]
    include!(concat!(env!("OUT_DIR"), "/any.rs"));
}

#[wstd::http_server]
async fn main(_request: Request<Body>) -> Result<Response<Body>, Error> {
    match sleep() {
        Ok(()) => Response::builder()
            .status(StatusCode::OK)
            .body(Body::empty())
            .map_err(Error::from),
        Err(err) => {
            eprintln!("{err:?}");
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::from(err.to_string()))
                .map_err(Error::from)
        }
    }
}

fn sleep() -> Result<(), anyhow::Error> {
    let seconds: u64 = std::env::var("SECONDS")?.parse()?;
    workflow::sleep_host_activity(generated::obelisk::types::time::Duration::Seconds(seconds))
        .map_err(|()| anyhow!("cancelled"))
}
