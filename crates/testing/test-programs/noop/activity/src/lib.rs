#![cfg(feature = "wasm")]

mod bindings;

struct Component;

impl crate::bindings::exports::testing::noop::noop::Guest for Component {
    fn noop() {}
}
