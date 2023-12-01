cargo_component_bindings::generate!();
use crate::bindings::exports::testing::fibo_workflow::workflow::Guest;

struct Component;

impl Guest for Component {
    fn fibow(n: u8, iterations: u8) -> u64 {
        let mut last = 0;
        for _ in 0..iterations {
            last = fibo(n);
        }
        last
    }

    fn fiboa(n: u8, iterations: u8) -> u64 {
        let mut last = 0;
        for _ in 0..iterations {
            last = crate::bindings::testing::fibo::fibo::fibo(n);
        }
        last
    }
}

pub fn fibo(n: u8) -> u64 {
    if n <= 1 {
        1
    } else {
        fibo(n - 1) + fibo(n - 2)
    }
}
