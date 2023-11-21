cargo_component_bindings::generate!();
use crate::bindings::my_org::my_workflow::host_activities::sleep;
use bindings::Guest;

struct Component;

impl Guest for Component {
    fn execute() -> String {
        sleep(1000);
        let res = crate::bindings::my_org::my_workflow::my_activity::send().unwrap();
        format!("Hello, {res}")
    }

    fn second() -> String {
        "second".to_string()
    }
}
