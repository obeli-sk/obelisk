use std::time::Duration;

mod bindings;

bindings::export!(Component with_types_in bindings);

struct Component;

impl crate::bindings::exports::testing::sleep::sleep::Guest for Component {
    fn sleep(nanos: u64) {
        std::thread::sleep(Duration::from_nanos(nanos));
    }

    fn sleep_loop(nanos: u64, iterations: u32) {
        for _ in 0..iterations {
            Self::sleep(nanos);
        }
    }
}
