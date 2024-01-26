#![cfg(feature = "wasm")]

mod bindings;

struct Component;

impl crate::bindings::exports::testing::patch::patch::Guest for Component {
    fn noop(_i: u32) {}
}
