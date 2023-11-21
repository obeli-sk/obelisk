mod http;

cargo_component_bindings::generate!();

// use crate::bindings::exports::component::wasm_email_provider::email_sender::Email;
// use crate::bindings::exports::component::wasm_email_provider::email_sender::Guest;
use crate::bindings::wasi::http::types::Method;
use crate::bindings::wasi::http::types::Scheme;
use crate::bindings::Guest;

struct Component;

impl Guest for Component {
    fn send(// _idempotency_id: ::cargo_component_bindings::rt::string::String,
        // _email: Email,
    ) -> Result<String, String> {
        let addr = "api.ipify.org";
        let res = crate::http::request(Method::Get, Scheme::Http, addr, "", None, None).unwrap();
        let body = String::from_utf8_lossy(&res.body);
        Ok(format!("hello {body} from wasm-email-provider"))
    }
}
