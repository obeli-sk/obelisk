mod http;

cargo_component_bindings::generate!();

use crate::bindings::wasi::http::types::Method;
use crate::bindings::wasi::http::types::Scheme;

struct Component;

impl crate::bindings::exports::testing::http::http_get::Guest for Component {
    fn get() -> Result<String, String> {
        let addr = "127.0.0.1:8080";
        let res = crate::http::request(Method::Get, Scheme::Http, addr, "/", None, None).unwrap();
        Ok(String::from_utf8_lossy(&res.body).to_string())
    }
}
