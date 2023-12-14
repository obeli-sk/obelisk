#![cfg(feature = "wasm")]

cargo_component_bindings::generate!();

struct Component;

impl crate::bindings::exports::testing::patch::patch::Guest for Component {
    fn noop(_i: u32) {}
}
