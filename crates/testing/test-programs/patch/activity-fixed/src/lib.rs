#![cfg(feature = "wasm")]

mod bindings;

bindings::export!(Component with_types_in bindings);

struct Component;

impl crate::bindings::exports::testing::patch::patch::Guest for Component {
    fn noop(_i: u32) {}
}
