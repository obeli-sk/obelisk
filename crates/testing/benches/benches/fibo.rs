use std::{
    fmt::Display,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, RawWaker, RawWakerVTable, Waker},
};

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use runtime::{
    activity::Activities,
    event_history::EventHistory,
    workflow::{ExecutionConfig, Workflow},
    FunctionFqn,
};
use wasmtime::component::Val;

fn fibonacci(n: u64) -> u64 {
    match n {
        0 => 1,
        1 => 1,
        n => fibonacci(n - 1) + fibonacci(n - 2),
    }
}

fn workflow(fibo_config: &FiboConfig) -> Workflow {
    let activities = Arc::new(
        run_await(Activities::new(
            test_programs_builder::TEST_PROGRAMS_FIBO_ACTIVITY.to_string(),
        ))
        .unwrap(),
    );
    run_await(Workflow::new_with_config(
        test_programs_builder::TEST_PROGRAMS_FIBO_WORKFLOW.to_string(),
        activities.clone(),
        ExecutionConfig {
            interrupt_on_activities: fibo_config.interrupt_on_activities,
        },
    ))
    .unwrap()
}

#[derive(Debug)]
struct FiboConfig {
    workflow_function: &'static str,
    n: u8,
    iterations: u16,
    interrupt_on_activities: bool,
}
impl FiboConfig {
    fn new(
        workflow_function: &'static str,
        n: u8,
        iterations: u16,
        interrupt_on_activities: bool,
    ) -> Self {
        Self {
            workflow_function,
            n,
            iterations,
            interrupt_on_activities,
        }
    }
}

impl Display for FiboConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{workflow_function}({n})*{iterations}{hot}",
            workflow_function = self.workflow_function,
            n = self.n,
            iterations = self.iterations,
            hot = if self.interrupt_on_activities {
                ""
            } else {
                "*hot*"
            },
        )
    }
}

fn benchmark_fast_functions(criterion: &mut Criterion) {
    let functions = vec![
        FiboConfig::new("fibow", 10, 10000, true),
        FiboConfig::new("fiboa", 10, 400, true),
        FiboConfig::new("fiboa", 10, 400, false),
    ];
    for fibo_config in functions {
        criterion.bench_function(&fibo_config.to_string(), |b| {
            let fqn = FunctionFqn::new(
                "testing:fibo-workflow/workflow",
                fibo_config.workflow_function,
            );
            let workflow = workflow(&fibo_config);
            b.iter(|| {
                let params = vec![Val::U8(fibo_config.n), Val::U16(fibo_config.iterations)];
                let mut event_history = EventHistory::new();
                run_await(workflow.execute_all(&mut event_history, &fqn, &params))
            })
        });
    }
}

fn benchmark_slow_functions(criterion: &mut Criterion) {
    criterion.bench_function("fibo(40)", |b| b.iter(|| fibonacci(black_box(40))));
    let functions = vec![
        FiboConfig::new("fibow", 40, 1, true),
        FiboConfig::new("fiboa", 40, 1, true),
        FiboConfig::new("fibow", 40, 10, true),
        FiboConfig::new("fiboa", 40, 10, true),
        FiboConfig::new("fiboa", 40, 10, false),
    ];
    for fibo_config in functions {
        criterion.bench_function(&fibo_config.to_string(), |b| {
            let fqn = FunctionFqn::new(
                "testing:fibo-workflow/workflow",
                fibo_config.workflow_function,
            );
            let workflow = workflow(&fibo_config);
            b.iter(|| {
                let params = vec![Val::U8(fibo_config.n), Val::U16(fibo_config.iterations)];
                let mut event_history = EventHistory::new();
                run_await(workflow.execute_all(&mut event_history, &fqn, &params))
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
