cargo_component_bindings::generate!();
use crate::bindings::my_org::my_workflow::host_activities::sleep;
use bindings::Guest;

struct Component;

impl Guest for Component {
    fn execute() -> String {
        sleep(1000);
        "Hello, World!".to_string()
    }
}
