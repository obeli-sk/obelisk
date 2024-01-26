#![cfg(feature = "wasm")]

mod bindings;

struct Component;

impl crate::bindings::exports::testing::noop_workflow::workflow::Guest for Component {
    fn noopa(iterations: u32) {
        for _ in 0..iterations {
            crate::bindings::testing::noop::noop::noop();
        }
    }

    fn noopha(iterations: u32) {
        for _ in 0..iterations {
            crate::bindings::my_org::workflow_engine::host_activities::noop();
        }
    }

    fn noopw(iterations: u32) {
        for i in 0..iterations {
            noop(black_box(i));
        }
    }
}

fn noop(i: u32) {
    black_box(i);
}

pub fn black_box<T>(dummy: T) -> T {
    unsafe {
        let ret = std::ptr::read_volatile(&dummy);
        std::mem::forget(dummy);
        ret
    }
}
