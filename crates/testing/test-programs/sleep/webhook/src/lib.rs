use crate::testing::sleep_workflow::workflow;
use anyhow::{Result, anyhow};
use wit_bindgen::generate;
use wstd::http::body::Body;
use wstd::http::{Error, Request, Response, StatusCode};
generate!({ generate_all });

#[wstd::http_server]
async fn main(_request: Request<Body>) -> Result<Response<Body>, Error> {
    match sleep().await {
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

async fn sleep() -> Result<(), anyhow::Error> {
    let seconds: u64 = std::env::var("SECONDS")?.parse()?;
    workflow::sleep_host_activity(obelisk::types::time::Duration::Seconds(seconds))
        .map_err(|()| anyhow!("cancelled"))
}
