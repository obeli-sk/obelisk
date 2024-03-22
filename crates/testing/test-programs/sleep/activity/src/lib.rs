#![cfg(feature = "wasm")]

use std::time::Duration;

mod bindings;

bindings::export!(Component with_types_in bindings);

struct Component;

impl crate::bindings::exports::testing::sleep::sleep::Guest for Component {
    fn sleep(millis: u64) {
        std::thread::sleep(Duration::from_millis(millis));
    }

    fn sleep_loop(millis: u64, iterations: u32) {
        for _ in 0..iterations {
            std::thread::sleep(Duration::from_millis(millis));
        }
    }
}

impl crate::bindings::exports::wasi::cli::run::Guest for Component {
    fn run() -> Result<(), ()> {
        std::thread::sleep(Duration::from_millis(100));
        Ok(())
    }
}
