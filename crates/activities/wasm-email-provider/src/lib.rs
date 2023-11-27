mod http;

cargo_component_bindings::generate!();

use crate::bindings::wasi::http::types::Method;
use crate::bindings::wasi::http::types::Scheme;

struct Component;

impl crate::bindings::exports::my_org::wasm_email_provider::email_sender::Guest for Component {
    fn send() -> Result<String, String> {
        let addr = "api.ipify.org";
        let res = crate::http::request(Method::Get, Scheme::Http, addr, "", None, None).unwrap();
        let body = String::from_utf8_lossy(&res.body);
        Ok(format!("hello {body} from wasm-email-provider"))
    }

    fn fibo10() -> Result<String, String> {
        Ok(fibo(10).to_string())
    }
    fn fibo40() -> Result<String, String> {
        Ok(fibo(40).to_string())
    }
}

fn fibo(n: usize) -> usize {
    if n <= 1 {
        1
    } else {
        fibo(n - 1) + fibo(n - 2)
    }
}
