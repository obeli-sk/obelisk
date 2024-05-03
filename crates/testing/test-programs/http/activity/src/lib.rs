#![cfg(feature = "wasm")]

mod http;

mod bindings;

use crate::bindings::wasi::http::types::Method;
use crate::bindings::wasi::http::types::Scheme;

bindings::export!(Component with_types_in bindings);

struct Component;

impl crate::bindings::exports::testing::http::http_get::Guest for Component {
    fn get(authority: String, path_with_query: String) -> Result<String, String> {
        crate::http::request(
            Method::Get,
            Scheme::Http,
            &authority,
            &path_with_query,
            None,
            None,
        )
        .map(|resp| String::from_utf8_lossy(&resp.body).to_string())
        .map_err(|err| format!("{err:?}"))
    }

    fn get_successful(authority: String, path_with_query: String) -> Result<String, String> {
        match crate::http::request(
            Method::Get,
            Scheme::Http,
            &authority,
            &path_with_query,
            None,
            None,
        ) {
            Ok(resp) => {
                if resp.status >= 200 && resp.status <= 299 {
                    Ok(String::from_utf8_lossy(&resp.body).to_string())
                } else {
                    Err(format!("wrong status code: {}", resp.status))
                }
            }
            Err(err) => Err(format!("{err:?}")),
        }
    }
}
