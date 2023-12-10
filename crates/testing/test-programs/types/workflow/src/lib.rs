cargo_component_bindings::generate!();
use crate::bindings::exports::testing::types_workflow::workflow::Guest;

struct Component;

impl Guest for Component {
    fn noopa(iterations: u16) {
        for _ in 0..iterations {
            crate::bindings::testing::types::types::noop();
        }
    }

    fn noopha(iterations: u16) {
        for _ in 0..iterations {
            crate::bindings::my_org::workflow_engine::host_activities::noop();
        }
    }

    fn noopw(iterations: u16) {
        for i in 0..iterations {
            noop(black_box(i));
        }
    }
}

fn noop(i: u16) {
    black_box(i);
}

pub fn black_box<T>(dummy: T) -> T {
    unsafe {
        let ret = std::ptr::read_volatile(&dummy);
        std::mem::forget(dummy);
        ret
    }
}
