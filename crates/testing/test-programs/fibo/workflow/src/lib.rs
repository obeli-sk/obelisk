#![cfg(feature = "wasm")]

mod bindings;

bindings::export!(Component with_types_in bindings);
struct Component;

pub fn black_box<T>(dummy: T) -> T {
    unsafe {
        let ret = std::ptr::read_volatile(&dummy);
        std::mem::forget(dummy);
        ret
    }
}

impl crate::bindings::exports::testing::fibo_workflow::workflow::Guest for Component {
    fn fibow(n: u8, iterations: u32) -> u64 {
        let mut last = 0;
        for _ in 0..iterations {
            last = fibo(black_box(n));
        }
        last
    }

    fn fiboa(n: u8, iterations: u32) -> u64 {
        let mut last = 0;
        for _ in 0..iterations {
            last = crate::bindings::testing::fibo::fibo::fibo(n);
        }
        last
    }
}

impl crate::bindings::exports::testing::fibo_workflow::workflow_nesting::Guest for Component {
    fn fibo_nested_workflow(n: u8) -> u64 {
        use crate::bindings::testing::fibo_workflow::workflow::fibow as fibo;
        if n <= 1 {
            1
        } else {
            fibo(n - 1, 1) + fibo(n - 2, 1)
        }
    }
}

pub fn fibo(n: u8) -> u64 {
    if n <= 1 {
        1
    } else {
        fibo(n - 1) + fibo(n - 2)
    }
}
