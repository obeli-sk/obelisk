#![cfg(feature = "wasm")]

mod http;

cargo_component_bindings::generate!();

use crate::bindings::wasi::http::types::Method;
use crate::bindings::wasi::http::types::Scheme;

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
}
