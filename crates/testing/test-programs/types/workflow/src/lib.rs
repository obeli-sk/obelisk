cargo_component_bindings::generate!();
use crate::bindings::exports::testing::types_workflow::workflow::Guest;

struct Component;

impl Guest for Component {
    fn noop(iterations: u8) {
        for _ in 0..iterations {
            crate::bindings::testing::types::types::noop();
        }
    }
}
