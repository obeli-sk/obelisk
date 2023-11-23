cargo_component_bindings::generate!();
use crate::bindings::my_org::workflow_engine::host_activities::{noop, sleep};
use bindings::Guest;

struct Component;

impl Guest for Component {
    fn execute() -> String {
        let res = crate::bindings::my_org::wasm_email_provider::email_sender::send().unwrap();
        sleep(1000);
        format!("Hello from workflow, {res}")
    }

    fn noop() -> String {
        for _ in 0..1000 {
            noop();
        }
        "done".to_string()
    }
}
