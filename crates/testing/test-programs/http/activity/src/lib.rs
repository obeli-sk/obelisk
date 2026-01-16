use generated::export;
use generated::exports::testing::http::http_get;
use generated::exports::testing::http::http_get::Guest;
use wstd::{
    http::{Body, Client, Method, Request},
    runtime::block_on,
};

mod generated {
    #![allow(clippy::empty_line_after_outer_attr)]
    include!(concat!(env!("OUT_DIR"), "/any.rs"));
}

struct Component;
export!(Component with_types_in generated);

async fn get_resp(url: String) -> Result<http_get::Response, anyhow::Error> {
    let request = Request::builder()
        .method(Method::GET)
        .uri(url)
        .body(Body::empty())?;

    let response = Client::new().send(request).await?;
    let status_code = response.status().as_u16();
    let mut response = response.into_body();
    let body = Vec::from(response.contents().await?);
    Ok(http_get::Response { body, status_code })
}

impl Guest for Component {
    fn get(url: String) -> Result<String, String> {
        let resp = Self::get_resp(url)?;
        Ok(String::from_utf8_lossy(&resp.body).into_owned())
    }

    fn get_successful(url: String) -> Result<String, String> {
        let resp = Self::get_resp(url)?;
        if resp.status_code >= 200 && resp.status_code <= 299 {
            Ok(String::from_utf8_lossy(&resp.body).into_owned())
        } else {
            assert!(resp.status_code != 418, "418 causes trap");
            Err(format!("wrong status code: {}", resp.status_code))
        }
    }

    fn get_resp(url: String) -> Result<http_get::Response, String> {
        block_on(async { get_resp(url).await }).map_err(|err| err.to_string())
    }

    fn get_stargazers() -> Result<http_get::Stargazers, String> {
        Ok(http_get::Stargazers {
            cursor: "cursor".to_string(),
            logins: "logins".to_string(),
        })
    }
}
