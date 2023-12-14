#![cfg(target_arch = "wasm32")]

cargo_component_bindings::generate!();

struct Component;

impl crate::bindings::exports::testing::patch::patch::Guest for Component {
    fn noop(i: u32) {
        if i == 5 {
            panic!("not implemented");
        }
    }
}
