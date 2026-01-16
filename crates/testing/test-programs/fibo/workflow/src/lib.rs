use generated::export;
use generated::exports::testing::fibo_workflow::workflow::Guest;
use generated::obelisk::workflow::workflow_support::join_set_create;
use generated::testing::{
    fibo::fibo::fibo as fibo_activity,
    fibo_obelisk_ext::fibo::{fibo_await_next, fibo_submit},
};

mod generated {
    #![allow(clippy::empty_line_after_outer_attr)]
    include!(concat!(env!("OUT_DIR"), "/any.rs"));
}

struct Component;
export!(Component with_types_in generated);

pub fn black_box<T>(dummy: T) -> T {
    unsafe {
        let ret = std::ptr::read_volatile(&raw const dummy);
        std::mem::forget(dummy);
        ret
    }
}

impl Guest for Component {
    fn fibow(n: u8, iterations: u32) -> Result<u64, ()> {
        let mut last = 0;
        for _ in 0..iterations {
            last = fibo(black_box(n));
        }
        Ok(last)
    }

    fn fiboa(n: u8, iterations: u32) -> Result<u64, ()> {
        let mut last = 0;
        for _ in 0..iterations {
            last = fibo_activity(n).unwrap();
        }
        Ok(last)
    }

    fn fiboa_concurrent(n: u8, iterations: u32) -> Result<u64, ()> {
        let join_set = join_set_create();
        for _ in 0..iterations {
            fibo_submit(&join_set, n);
        }
        let mut last = 0;
        for _ in 0..iterations {
            last = fibo_await_next(&join_set).unwrap().1.unwrap();
        }
        Ok(last)
    }
}

#[must_use]
pub fn fibo(n: u8) -> u64 {
    if n == 0 {
        0
    } else if n == 1 {
        1
    } else {
        fibo(n - 1) + fibo(n - 2)
    }
}
