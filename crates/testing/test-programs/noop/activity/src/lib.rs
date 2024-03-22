#![cfg(feature = "wasm")]

mod bindings;

bindings::export!(Component with_types_in bindings);

struct Component;

impl crate::bindings::exports::testing::noop::noop::Guest for Component {
    fn noop() {}

    fn noop2() {}
}
