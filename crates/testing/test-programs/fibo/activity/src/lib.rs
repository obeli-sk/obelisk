use generated::export;
use generated::exports::testing::fibo::fibo::Guest;

mod generated {
    #![allow(clippy::all)]
    include!(concat!(env!("OUT_DIR"), "/any.rs"));
}

struct Component;
export!(Component with_types_in generated);

impl Guest for Component {
    fn fibo(n: u8) -> Result<u64, ()> {
        if n > 40 {
            // Return error for large n (used for testing error variant handling)
            Err(())
        } else {
            Ok(fibo(n))
        }
    }
}

fn fibo(n: u8) -> u64 {
    if n == 0 {
        0
    } else if n == 1 {
        1
    } else {
        fibo(n - 1) + fibo(n - 2)
    }
}
