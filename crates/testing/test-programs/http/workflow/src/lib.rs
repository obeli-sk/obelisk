#![cfg(target_arch = "wasm32")]

cargo_component_bindings::generate!();
struct Component;

impl crate::bindings::exports::testing::http_workflow::workflow::Guest for Component {
    fn execute(port: u16) -> String {
        crate::bindings::testing::http::http_get::get(&format!("127.0.0.1:{port}"), "/").unwrap()
    }
}
