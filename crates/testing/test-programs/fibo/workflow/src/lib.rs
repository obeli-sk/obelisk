#![cfg(feature = "wasm")]

mod bindings;

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

pub fn fibo(n: u8) -> u64 {
    if n <= 1 {
        1
    } else {
        fibo(n - 1) + fibo(n - 2)
    }
}
