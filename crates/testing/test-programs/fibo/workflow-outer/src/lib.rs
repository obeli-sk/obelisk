use generated::export;
use generated::exports::testing::fibo_workflow_outer::workflow::Guest as GuestNesting;
use generated::obelisk::workflow::workflow_support::join_set_create;

mod generated {
    #![allow(clippy::all)]
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

impl GuestNesting for Component {
    // Start 2 child workflows each starting 2 child workflows...
    fn fibo_nested_workflow(n: u8) -> Result<u64, ()> {
        use generated::testing::fibo_workflow_outer::workflow::fibo_nested_workflow as imported_fibo_nested_workflow;
        if n <= 1 {
            Ok(1)
        } else {
            Ok(imported_fibo_nested_workflow(n - 1).unwrap()
                + imported_fibo_nested_workflow(n - 2).unwrap())
        }
    }

    // Calls `fiboa` workflow concurrently.
    // Used for testing pressure of the parallel children results on the parent execution.
    // Having all children compete for write their result to a single parent, like
    // `fiboa_concurrent`, is significantly slower than having multiple parent executions
    // behind a single topmost parent.
    fn fibo_start_fiboas(n: u8, fiboas: u32, iterations_per_fiboa: u32) -> Result<u64, ()> {
        use generated::testing::fibo_workflow_obelisk_ext::workflow::{
            fiboa_concurrent_await_next, fiboa_concurrent_submit,
        };
        let join_set = join_set_create();
        for _ in 0..fiboas {
            fiboa_concurrent_submit(&join_set, n, iterations_per_fiboa);
        }
        let mut last = 0;
        for _ in 0..fiboas {
            last = fiboa_concurrent_await_next(&join_set).unwrap().1.unwrap();
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
