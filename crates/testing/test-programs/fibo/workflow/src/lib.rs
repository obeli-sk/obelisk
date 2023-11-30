cargo_component_bindings::generate!();
use crate::bindings::exports::testing::fibo_workflow::workflow::Guest;

struct Component;

impl Guest for Component {
    fn fibo10w() -> String {
        fibo(10).to_string()
    }

    fn fibo10a() -> String {
        crate::bindings::testing::fibo::fibo::fibo10().unwrap()
    }

    fn fibo10a_times40() -> String {
        let mut last = String::new();
        for _ in 0..40 {
            last = Self::fibo10a();
        }
        last
    }

    fn fibo10w_times40() -> String {
        let mut last = String::new();
        for _ in 0..40 {
            last = Self::fibo10w();
        }
        last
    }

    fn fibo40w() -> String {
        fibo(40).to_string()
    }

    fn fibo40a() -> String {
        crate::bindings::testing::fibo::fibo::fibo40().unwrap()
    }

    fn fibo40a_times10() -> String {
        let mut last = String::new();
        for _ in 0..10 {
            last = Self::fibo40a();
        }
        last
    }

    fn fibo40w_times10() -> String {
        let mut last = String::new();
        for _ in 0..10 {
            last = Self::fibo40w();
        }
        last
    }
}

pub fn fibo(n: usize) -> usize {
    if n <= 1 {
        1
    } else {
        fibo(n - 1) + fibo(n - 2)
    }
}
