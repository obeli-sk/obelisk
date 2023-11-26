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

    fn fibo10() -> String {
        fibo(10).to_string()
    }

    fn fibo10x40() -> String {
        let mut last = String::default();
        for _ in 0..40 {
            let v = Self::fibo10();
            if last != v {
                last = v;
            }
        }
        last
    }

    fn fibo40() -> String {
        fibo(40).to_string()
    }

    fn fibo40x10() -> String {
        let mut last = String::new();
        for _ in 0..10 {
            last = Self::fibo40();
        }
        last
    }
}

fn fibo(n: usize) -> usize {
    if n <= 1 {
        1
    } else {
        fibo(n - 1) + fibo(n - 2)
    }
}
