mod http;

cargo_component_bindings::generate!();

use crate::bindings::wasi::http::types::Method;
use crate::bindings::wasi::http::types::Scheme;

struct Component;

impl crate::bindings::exports::component::wasm_email_provider::email_sender::Guest for Component {
    fn send() -> Result<String, String> {
        let addr = "api.ipify.org";
        let res = crate::http::request(Method::Get, Scheme::Http, addr, "", None, None).unwrap();
        let body = String::from_utf8_lossy(&res.body);
        Ok(format!("hello {body} from wasm-email-provider"))
    }
}
