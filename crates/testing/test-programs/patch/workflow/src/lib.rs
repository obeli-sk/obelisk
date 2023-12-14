#![cfg(feature = "wasm")]

cargo_component_bindings::generate!();

struct Component;

impl crate::bindings::exports::testing::patch_workflow::workflow::Guest for Component {
    fn noopa(iterations: u32) {
        for idx in 0..iterations {
            crate::bindings::testing::patch::patch::noop(idx);
        }
    }
}
