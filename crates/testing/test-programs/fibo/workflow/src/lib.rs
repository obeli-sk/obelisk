use exports::testing::fibo_workflow::{workflow::Guest, workflow_nesting::Guest as GuestNesting};
use obelisk::workflow::host_activities::new_join_set;
use testing::{
    fibo::fibo::fibo as fibo_activity,
    fibo_obelisk_ext::fibo::{fibo_await_next, fibo_submit},
};
use wit_bindgen::generate;

generate!({ generate_all });
struct Component;
export!(Component);

pub fn black_box<T>(dummy: T) -> T {
    unsafe {
        let ret = std::ptr::read_volatile(&dummy);
        std::mem::forget(dummy);
        ret
    }
}

impl Guest for Component {
    fn fibow(n: u8, iterations: u32) -> u64 {
        let mut last = 0;
        for _ in 0..iterations {
            last = fibo(black_box(n));
        }
        last
    }

    fn fiboa(n: u8, iterations: u32) -> u64 {
        let mut last = 0;
        for _ in 0..iterations {
            last = fibo_activity(n);
        }
        last
    }

    fn fiboa_concurrent(n: u8, iterations: u32) -> u64 {
        let join_set_id = new_join_set();
        for _ in 0..iterations {
            fibo_submit(&join_set_id, n);
        }
        let mut last = 0;
        for _ in 0..iterations {
            last = fibo_await_next(&join_set_id).unwrap().1;
        }
        last
    }
}

impl GuestNesting for Component {
    // Start 2 child workflows each starting 2 child workflows...
    fn fibo_nested_workflow(n: u8) -> u64 {
        use testing::fibo_workflow::workflow_nesting::fibo_nested_workflow as imported_fibo_nested_workflow;
        if n <= 1 {
            1
        } else {
            imported_fibo_nested_workflow(n - 1) + imported_fibo_nested_workflow(n - 2)
        }
    }

    // Calls `fiboa` workflow concurrently.
    // Used for testing pressure of the parallel children results on the parent execution.
    // Having all children compete for write their result to a single parent, like
    // `fiboa_concurrent`, is significantly slower than having multiple parent executions
    // behind a single topmost parent.
    fn fibo_start_fiboas(n: u8, fiboas: u32, iterations_per_fiboa: u32) -> u64 {
        use testing::fibo_workflow_obelisk_ext::workflow::{
            fiboa_concurrent_await_next, fiboa_concurrent_submit,
        };
        let join_set_id = new_join_set();
        for _ in 0..fiboas {
            fiboa_concurrent_submit(&join_set_id, n, iterations_per_fiboa);
        }
        let mut last = 0;
        for _ in 0..fiboas {
            last = fiboa_concurrent_await_next(&join_set_id).unwrap().1;
        }
        last
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
