cargo_component_bindings::generate!();

struct Component;

impl crate::bindings::exports::testing::fibo::fibo::Guest for Component {
    fn fibo10() -> Result<String, String> {
        Ok(fibo(10).to_string())
    }
    fn fibo40() -> Result<String, String> {
        Ok(fibo(40).to_string())
    }
}

fn fibo(n: usize) -> usize {
    if n <= 1 {
        1
    } else {
        fibo(n - 1) + fibo(n - 2)
    }
}
