#![cfg(feature = "wasm")]

cargo_component_bindings::generate!();

struct Component;

impl crate::bindings::exports::testing::fibo::fibo::Guest for Component {
    fn fibo(n: u8) -> u64 {
        fibo(n)
    }
}

fn fibo(n: u8) -> u64 {
    if n <= 1 {
        1
    } else {
        fibo(n - 1) + fibo(n - 2)
    }
}
