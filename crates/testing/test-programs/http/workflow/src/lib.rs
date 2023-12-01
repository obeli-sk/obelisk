cargo_component_bindings::generate!();
use crate::bindings::exports::testing::http_workflow::workflow::Guest;

struct Component;

impl Guest for Component {
    fn execute(port: u16) -> String {
        crate::bindings::testing::http::http_get::get(&format!("127.0.0.1:{port}"), "/").unwrap()
    }
}
