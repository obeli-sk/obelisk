#![cfg(feature = "wasm")]

mod bindings;

struct Component;

impl crate::bindings::exports::testing::patch::patch::Guest for Component {
    fn noop(i: u32) {
        if i == 5 {
            panic!("not implemented");
        }
    }
}
