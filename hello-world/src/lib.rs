cargo_component_bindings::generate!();

use bindings::Guest;

struct Component;

impl Guest for Component {
    fn execute() -> String {
        bindings::sleep(1000);
        "Hello, World!".to_string()
    }
}
