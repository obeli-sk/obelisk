cargo_component_bindings::generate!();
use crate::bindings::exports::testing::http_workflow::workflow::Guest;

struct Component;

impl Guest for Component {
    fn execute() -> String {
        crate::bindings::testing::http::http_get::get().unwrap()
    }
}
