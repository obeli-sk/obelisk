use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, RawWaker, RawWakerVTable, Waker},
};

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use runtime::{activity::Activities, event_history::EventHistory, workflow::Workflow};
use wasmtime::component::Val;

fn fibonacci(n: u64) -> u64 {
    match n {
        0 => 1,
        1 => 1,
        n => fibonacci(n - 1) + fibonacci(n - 2),
    }
}

fn workflow() -> Workflow {
    let activities = Arc::new(
        run_await(Activities::new(
            test_programs_builder::TEST_PROGRAMS_FIBO_ACTIVITY.to_string(),
        ))
        .unwrap(),
    );
    run_await(Workflow::new(
        test_programs_builder::TEST_PROGRAMS_FIBO_WORKFLOW.to_string(),
        activities.clone(),
    ))
    .unwrap()
}

fn benchmark_fast_functions(criterion: &mut Criterion) {
    criterion.bench_function("fibo(10)", |b| b.iter(|| fibonacci(black_box(10))));
    criterion.bench_function("fibo(40)", |b| b.iter(|| fibonacci(black_box(40))));
    let workflow = workflow();
    let functions = vec![
        ("fibow", 10, 1),
        ("fiboa", 10, 1),
        ("fibow", 10, 40),
        ("fiboa", 10, 40),
    ];
    for (function, n, iterations) in functions {
        criterion.bench_function(&format!("{function}({n})*{iterations}"), |b| {
            b.iter(|| {
                let params = vec![Val::U8(n), Val::U8(iterations)];
                let mut event_history = EventHistory::new();
                run_await(workflow.execute_all(
                    &mut event_history,
                    Some("testing:fibo-workflow/workflow"),
                    function,
                    &params,
                ))
            })
        });
    }
}

fn benchmark_slow_functions(criterion: &mut Criterion) {
    let workflow = workflow();
    let functions = vec![
        ("fibow", 40, 1),
        ("fiboa", 40, 1),
        ("fibow", 40, 10),
        ("fiboa", 40, 10),
    ];
    for (function, n, iterations) in functions {
        criterion.bench_function(&format!("{function}({n})*{iterations}"), |b| {
            b.iter(|| {
                let params = vec![Val::U8(n), Val::U8(iterations)];
                let mut event_history = EventHistory::new();
                run_await(workflow.execute_all(
                    &mut event_history,
                    Some("testing:fibo-workflow/workflow"),
                    function,
                    &params,
                ))
            })
        });
    }
}

criterion_group! {
  name = fast_benches;
  config = Criterion::default();
  targets = benchmark_fast_functions
}
criterion_group! {
  name = slow_benches;
  config = Criterion::default().sample_size(10);
  targets = benchmark_slow_functions
}
criterion_main!(fast_benches, slow_benches);

fn run_await<F: Future>(future: F) -> F::Output {
    let mut f = Pin::from(Box::new(future));
    let waker = dummy_waker();
    let mut cx = Context::from_waker(&waker);
    loop {
        match f.as_mut().poll(&mut cx) {
            Poll::Ready(val) => break val,
            Poll::Pending => {}
        }
    }
}

fn dummy_waker() -> Waker {
    return unsafe { Waker::from_raw(clone(5 as *const _)) };

    unsafe fn clone(ptr: *const ()) -> RawWaker {
        assert_eq!(ptr as usize, 5);
        const VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop);
        RawWaker::new(ptr, &VTABLE)
    }

    unsafe fn wake(ptr: *const ()) {
        assert_eq!(ptr as usize, 5);
    }

    unsafe fn wake_by_ref(ptr: *const ()) {
        assert_eq!(ptr as usize, 5);
    }

    unsafe fn drop(ptr: *const ()) {
        assert_eq!(ptr as usize, 5);
    }
}
