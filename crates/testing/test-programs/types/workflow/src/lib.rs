#![cfg(target_arch = "wasm32")]

cargo_component_bindings::generate!();

struct Component;

impl crate::bindings::exports::testing::types_workflow::workflow::Guest for Component {
    fn noopa(iterations: u32) {
        for _ in 0..iterations {
            crate::bindings::testing::types::types::noop();
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
