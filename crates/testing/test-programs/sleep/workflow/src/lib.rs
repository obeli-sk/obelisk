#![cfg(feature = "wasm")]

mod bindings;

bindings::export!(Component with_types_in bindings);

struct Component;

impl crate::bindings::exports::testing::sleep_workflow::workflow::Guest for Component {
    fn sleep_host_activity(millis: u32) {
        crate::bindings::obelisk::workflow::host_activities::sleep(millis);
    }

    fn sleep_activity(millis: u32) {
        crate::bindings::testing::sleep::sleep::sleep(millis);
    }
}
