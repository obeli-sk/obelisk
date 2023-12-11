use std::{
    fmt::Display,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, RawWaker, RawWakerVTable, Waker},
};

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use runtime::{
    activity::{Activities, ActivityConfig, ActivityPreload},
    event_history::EventHistory,
    workflow::{AsyncActivityBehavior, Workflow, WorkflowConfig},
    FunctionFqn,
};
use wasmtime::component::Val;

const ACTIVITY_CONFIG_COLD: ActivityConfig = ActivityConfig {
    preload: ActivityPreload::Preinstance,
};
const ACTIVITY_CONFIG_HOT: ActivityConfig = ActivityConfig {
    preload: ActivityPreload::Instance,
};
const WORKFLOW_CONFIG_COLD: WorkflowConfig = WorkflowConfig {
    async_activity_behavior: AsyncActivityBehavior::Restart,
};
const WORKFLOW_CONFIG_HOT: WorkflowConfig = WorkflowConfig {
    async_activity_behavior: AsyncActivityBehavior::KeepWaiting,
};

fn noop_workflow(activity_config: &ActivityConfig, workflow_config: &WorkflowConfig) -> Workflow {
    let activities = Arc::new(
        run_await(Activities::new_with_config(
            test_programs_builder::TEST_PROGRAMS_TYPES_ACTIVITY.to_string(),
            activity_config,
        ))
        .unwrap(),
    );
    run_await(Workflow::new_with_config(
        test_programs_builder::TEST_PROGRAMS_TYPES_WORKFLOW.to_string(),
        activities.clone(),
        workflow_config,
    ))
    .unwrap()
}

fn fibonacci(n: u64) -> u64 {
    match n {
        0 => 1,
        1 => 1,
        n => fibonacci(n - 1) + fibonacci(n - 2),
    }
}

fn fibo_workflow(fibo_config: &FiboConfig) -> Workflow {
    let activities = Arc::new(
        run_await(Activities::new_with_config(
            test_programs_builder::TEST_PROGRAMS_FIBO_ACTIVITY.to_string(),
            &fibo_config.activity_config,
        ))
        .unwrap(),
    );
    run_await(Workflow::new_with_config(
        test_programs_builder::TEST_PROGRAMS_FIBO_WORKFLOW.to_string(),
        activities.clone(),
        &fibo_config.workflow_config,
    ))
    .unwrap()
}

#[derive(Debug)]
struct FiboConfig {
    workflow_function: &'static str,
    n: u8,
    iterations: u32,
    activity_config: ActivityConfig,
    workflow_config: WorkflowConfig,
}
impl FiboConfig {
    fn new(
        workflow_function: &'static str,
        n: u8,
        iterations: u32,
        activity_config: ActivityConfig,
        workflow_config: WorkflowConfig,
    ) -> Self {
        Self {
            workflow_function,
            n,
            iterations,
            activity_config,
            workflow_config,
        }
    }
}

impl Display for FiboConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{workflow_function}({n})*{iterations}{hot_or_cold}",
            workflow_function = self.workflow_function,
            n = self.n,
            iterations = self.iterations,
            hot_or_cold = hot_or_cold(&self.activity_config, &self.workflow_config),
        )
    }
}

fn hot_or_cold(activity_config: &ActivityConfig, workflow_config: &WorkflowConfig) -> String {
    let mut v = Vec::new();
    if activity_config == &ACTIVITY_CONFIG_HOT {
        v.push("activity_hot")
    }
    if workflow_config == &WORKFLOW_CONFIG_HOT {
        v.push("workflow_hot")
    }
    v.join(",")
}

fn benchmark_noop_functions(criterion: &mut Criterion) {
    let functions = vec![
        ("noopw", 1, ACTIVITY_CONFIG_COLD, WORKFLOW_CONFIG_COLD),
        ("noopw", 100, ACTIVITY_CONFIG_COLD, WORKFLOW_CONFIG_COLD),
        ("noopa", 1, ACTIVITY_CONFIG_COLD, WORKFLOW_CONFIG_HOT),
        ("noopa", 100, ACTIVITY_CONFIG_COLD, WORKFLOW_CONFIG_HOT),
        ("noopa", 100, ACTIVITY_CONFIG_HOT, WORKFLOW_CONFIG_HOT),
        ("noopha", 1, ACTIVITY_CONFIG_COLD, WORKFLOW_CONFIG_COLD),
        ("noopha", 100, ACTIVITY_CONFIG_COLD, WORKFLOW_CONFIG_HOT),
    ];
    for (workflow_function, iterations, activity_config, workflow_config) in functions {
        criterion.bench_function(
            &format!(
                "{workflow_function}*{iterations}{hot_or_cold}",
                hot_or_cold = hot_or_cold(&activity_config, &workflow_config)
            ),
            |b| {
                let fqn = FunctionFqn::new("testing:types-workflow/workflow", workflow_function);
                let workflow = noop_workflow(&activity_config, &workflow_config);
                b.iter(|| {
                    let params = vec![Val::U32(iterations)];
                    let mut event_history = EventHistory::new();
                    run_await(workflow.execute_all(&mut event_history, &fqn, &params)).unwrap()
                })
            },
        );
    }
}

fn benchmark_fibo_fast_functions(criterion: &mut Criterion) {
    criterion.bench_function("fibo(10)", |b| b.iter(|| fibonacci(black_box(10))));
    criterion.bench_function("fibo(10)*10", |b| {
        b.iter(|| {
            for _ in 0..10 {
                fibonacci(black_box(10));
            }
        })
    });
    criterion.bench_function("fibo(10)*100", |b| {
        b.iter(|| {
            for _ in 0..100 {
                fibonacci(black_box(10));
            }
        })
    });
    let functions = vec![
        FiboConfig::new("fibow", 10, 1, ACTIVITY_CONFIG_COLD, WORKFLOW_CONFIG_COLD),
        FiboConfig::new("fibow", 10, 100, ACTIVITY_CONFIG_COLD, WORKFLOW_CONFIG_COLD),
        FiboConfig::new("fiboa", 10, 1, ACTIVITY_CONFIG_COLD, WORKFLOW_CONFIG_COLD),
        FiboConfig::new("fiboa", 10, 100, ACTIVITY_CONFIG_COLD, WORKFLOW_CONFIG_COLD),
        FiboConfig::new("fiboa", 10, 100, ACTIVITY_CONFIG_COLD, WORKFLOW_CONFIG_HOT),
        FiboConfig::new("fiboa", 10, 100, ACTIVITY_CONFIG_HOT, WORKFLOW_CONFIG_HOT),
    ];
    for fibo_config in functions {
        criterion.bench_function(&fibo_config.to_string(), |b| {
            let fqn = FunctionFqn::new(
                "testing:fibo-workflow/workflow",
                fibo_config.workflow_function,
            );
            let workflow = fibo_workflow(&fibo_config);
            b.iter(|| {
                let params = vec![Val::U8(fibo_config.n), Val::U32(fibo_config.iterations)];
                let mut event_history = EventHistory::new();
                run_await(workflow.execute_all(&mut event_history, &fqn, &params)).unwrap()
            })
        });
    }
}

fn benchmark_fibo_slow_functions(criterion: &mut Criterion) {
    criterion.bench_function("fibo(40)", |b| b.iter(|| fibonacci(black_box(40))));
    let functions = vec![
        FiboConfig::new("fibow", 40, 1, ACTIVITY_CONFIG_COLD, WORKFLOW_CONFIG_COLD),
        FiboConfig::new("fiboa", 40, 1, ACTIVITY_CONFIG_COLD, WORKFLOW_CONFIG_COLD),
        FiboConfig::new("fibow", 40, 10, ACTIVITY_CONFIG_COLD, WORKFLOW_CONFIG_COLD),
        FiboConfig::new("fiboa", 40, 10, ACTIVITY_CONFIG_COLD, WORKFLOW_CONFIG_COLD),
    ];
    for fibo_config in functions {
        criterion.bench_function(&fibo_config.to_string(), |b| {
            let fqn = FunctionFqn::new(
                "testing:fibo-workflow/workflow",
                fibo_config.workflow_function,
            );
            let workflow = fibo_workflow(&fibo_config);
            b.iter(|| {
                let params = vec![Val::U8(fibo_config.n), Val::U32(fibo_config.iterations)];
                let mut event_history = EventHistory::new();
                run_await(workflow.execute_all(&mut event_history, &fqn, &params)).unwrap()
            })
        });
    }
}

criterion_group! {
  name = noop_benches;
  config = Criterion::default();
  targets = benchmark_noop_functions
}
criterion_group! {
  name = fibo_fast_benches;
  config = Criterion::default();
  targets = benchmark_fibo_fast_functions
}
criterion_group! {
  name = fibo_slow_benches;
  config = Criterion::default().sample_size(10);
  targets = benchmark_fibo_slow_functions
}
criterion_main!(noop_benches, fibo_fast_benches, fibo_slow_benches);

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
