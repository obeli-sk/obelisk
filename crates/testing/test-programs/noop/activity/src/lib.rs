#![cfg(feature = "wasm")]

mod bindings;

struct Component;

impl crate::bindings::exports::testing::types::types::Guest for Component {
    fn noop() {}
}
