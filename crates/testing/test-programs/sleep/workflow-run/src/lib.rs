#![cfg(feature = "wasm")]

cargo_component_bindings::generate!();

struct Component;

impl crate::bindings::exports::testing::sleep_workflow_run::workflow::Guest for Component {
    fn sleep() {
        crate::bindings::wasi::cli::run::run().unwrap();
    }
}
