use std::time::Duration;

mod bindings;

bindings::export!(Component with_types_in bindings);

struct Component;

impl crate::bindings::exports::testing::sleep::sleep::Guest for Component {
    fn sleep(millis: u32) {
        std::thread::sleep(Duration::from_millis(millis as u64));
    }

    fn sleep_loop(millis: u32, iterations: u32) {
        for _ in 0..iterations {
            Self::sleep(millis);
        }
    }
}
