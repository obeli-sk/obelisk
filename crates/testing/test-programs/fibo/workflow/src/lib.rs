mod bindings;

bindings::export!(Component with_types_in bindings);
struct Component;

pub fn black_box<T>(dummy: T) -> T {
    unsafe {
        let ret = std::ptr::read_volatile(&dummy);
        std::mem::forget(dummy);
        ret
    }
}

impl crate::bindings::exports::testing::fibo_workflow::workflow::Guest for Component {
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
            last = crate::bindings::testing::fibo::fibo::fibo(n);
        }
        last
    }

    fn fiboa_concurrent(n: u8, iterations: u32) -> u64 {
        let join_set_id = bindings::obelisk::workflow::host_activities::new_join_set();
        for _ in 0..iterations {
            crate::bindings::testing::fibo_obelisk_ext::fibo::fibo_future(&join_set_id, n);
            // FIXME: fibo_submit
        }
        let mut last = 0;
        for _ in 0..iterations {
            last = crate::bindings::testing::fibo_obelisk_ext::fibo::fibo_await_next(&join_set_id);
        }
        last
    }
}

impl crate::bindings::exports::testing::fibo_workflow::workflow_nesting::Guest for Component {
    fn fibo_nested_workflow(n: u8) -> u64 {
        // FIXME: n, iterations_per_fiboa, fiboa_count + concurrent version
        use crate::bindings::testing::fibo_workflow::workflow_nesting::fibo_nested_workflow as fibo;
        if n <= 1 {
            1
        } else {
            fibo(n - 1) + fibo(n - 2)
        }
    }

    fn fibo_start_fiboas(n: u8, fiboas: u32, iterations_per_fiboa: u32) -> u64 {
        use crate::bindings::testing::fibo_workflow_obelisk_ext::workflow::{
            fiboa_await_next, fiboa_future,
        };
        let join_set_id = bindings::obelisk::workflow::host_activities::new_join_set();
        for _ in 0..fiboas {
            fiboa_future(&join_set_id, n, iterations_per_fiboa);
        }
        let mut last = 0;
        for _ in 0..fiboas {
            last = fiboa_await_next(&join_set_id);
        }
        last
    }
}

pub fn fibo(n: u8) -> u64 {
    if n <= 1 {
        1
    } else {
        fibo(n - 1) + fibo(n - 2)
    }
}
