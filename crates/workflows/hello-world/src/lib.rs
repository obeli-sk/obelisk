mod fibo;

use fibo::fibo;

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
        for _ in 0..1_000_000 {
            noop();
        }
        "done".to_string()
    }

    // fibo withn the workflow
    fn fibo10w() -> String {
        fibo(10).to_string()
    }

    fn fibo40w() -> String {
        fibo(40).to_string()
    }

    fn fibo10x40w() -> String {
        let mut last = String::default();
        for _ in 0..40 {
            let v = Self::fibo10w();
            if last != v {
                last = v;
            }
        }
        last
    }

    fn fibo40x10w() -> String {
        let mut last = String::new();
        for _ in 0..10 {
            last = Self::fibo40w();
        }
        last
    }
    // fibo activities
    fn fibo10a() -> String {
        crate::bindings::my_org::wasm_email_provider::email_sender::fibo10().unwrap()
    }

    fn fibo40a() -> String {
        crate::bindings::my_org::wasm_email_provider::email_sender::fibo40().unwrap()
    }

    fn fibo10x40a() -> String {
        let mut last = String::default();
        for _ in 0..40 {
            let v = crate::bindings::my_org::wasm_email_provider::email_sender::fibo10().unwrap();
            if last != v {
                last = v;
            }
        }
        last
    }

    fn fibo40x10a() -> String {
        let mut last = String::new();
        for _ in 0..10 {
            last = crate::bindings::my_org::wasm_email_provider::email_sender::fibo40().unwrap();
        }
        last
    }
}
