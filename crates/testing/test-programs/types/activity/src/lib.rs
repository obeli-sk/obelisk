#![cfg(target_arch = "wasm32")]

cargo_component_bindings::generate!();

struct Component;

impl crate::bindings::exports::testing::types::types::Guest for Component {
    fn noop() {}
}
