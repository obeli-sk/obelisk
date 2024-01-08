#![cfg(feature = "wasm")]

use std::time::Duration;

cargo_component_bindings::generate!();

struct Component;

impl crate::bindings::exports::testing::sleep::sleep::Guest for Component {
    fn sleep(millis: u64) {
        std::thread::sleep(Duration::from_millis(millis));
    }
}

impl crate::bindings::exports::wasi::cli::run::Guest for Component {
    fn run() -> Result<(), ()> {
        std::thread::sleep(Duration::from_millis(100));
        Ok(())
    }
}
