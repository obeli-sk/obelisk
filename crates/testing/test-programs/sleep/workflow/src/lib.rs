#![cfg(feature = "wasm")]

cargo_component_bindings::generate!();

struct Component;

impl crate::bindings::exports::testing::sleep_workflow::workflow::Guest for Component {
    fn sleep(millis: u64) {
        crate::bindings::my_org::workflow_engine::host_activities::sleep(millis);
    }
}
