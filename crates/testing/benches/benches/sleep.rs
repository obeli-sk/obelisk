use concepts::{workflow_id::WorkflowId, FunctionFqn};
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use lazy_static::lazy_static;
use runtime::{
    activity::ACTIVITY_CONFIG_HOT,
    database::Database,
    event_history::EventHistory,
    runtime::{Runtime, RuntimeBuilder},
    workflow::WORKFLOW_CONFIG_HOT,
};
use std::sync::{atomic::Ordering, Arc};
use tokio::{process::Command, sync::Mutex};
use wasmtime::component::Val;

const DB_BUFFER_CAPACITY: usize = 100;
const ELEMENTS_PER_ITERATION: u64 = 100;
const SLEEP_MILLIS: u64 = 1;

lazy_static! {
    static ref RT: tokio::runtime::Runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_time()
        .enable_io()
        .build()
        .expect("Should create a tokio runtime");
}

fn setup_runtime() -> Runtime {
    RT.block_on(async {
        let mut runtime = RuntimeBuilder::default();
        runtime
            .add_activity(
                test_programs_sleep_activity_builder::TEST_PROGRAMS_SLEEP_ACTIVITY.to_string(),
                &ACTIVITY_CONFIG_HOT,
            )
            .await
            .unwrap();
        runtime
            .build(
                test_programs_sleep_workflow_builder::TEST_PROGRAMS_SLEEP_WORKFLOW.to_string(),
                &WORKFLOW_CONFIG_HOT,
            )
            .await
            .unwrap()
    })
}

static COUNTER: std::sync::atomic::AtomicU16 = std::sync::atomic::AtomicU16::new(0);

fn benchmark_sleep(criterion: &mut Criterion) {
    let database = Database::new(DB_BUFFER_CAPACITY, DB_BUFFER_CAPACITY);
    let runtime = setup_runtime();
    let _abort_handle = RT.block_on(async { runtime.spawn(&database) });

    let mut group = criterion.benchmark_group("sleep");
    group.throughput(Throughput::Elements(ELEMENTS_PER_ITERATION));
    for function in ["sleep-host-activity", "sleep-activity"] {
        group.bench_function(function, |b| {
            let fqn = FunctionFqn::new(
                "testing:sleep-workflow/workflow".to_string(),
                function.to_string(),
            );
            b.to_async::<&tokio::runtime::Runtime>(&RT).iter(|| {
                let params = Arc::new(vec![Val::U64(SLEEP_MILLIS)]);
                let fqn = fqn.clone();
                let workflow_scheduler = database.workflow_scheduler();
                async move {
                    let mut futures = Vec::new();
                    for _ in 0..ELEMENTS_PER_ITERATION {
                        let event_history = Arc::new(Mutex::new(EventHistory::default()));
                        let workflow_id =
                            WorkflowId::new(COUNTER.fetch_add(1, Ordering::SeqCst).to_string());
                        futures.push(workflow_scheduler.schedule_workflow(
                            workflow_id,
                            event_history,
                            fqn.clone(),
                            params.clone(),
                        ));
                    }
                    futures_util::future::join_all(futures)
                        .await
                        .into_iter()
                        .collect::<Result<Vec<_>, _>>()
                        .unwrap();
                }
            });
        });
    }
    group.finish();
}

fn benchmark_sleep_process(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("sleep-process");
    group.throughput(Throughput::Elements(ELEMENTS_PER_ITERATION));
    let mut which = std::process::Command::new("which")
        .arg("sleep")
        .output()
        .unwrap()
        .stdout;
    which.pop();
    for exe in ["sleep", &String::from_utf8_lossy(which.as_slice())] {
        group.bench_function(exe, |b| {
            b.to_async::<&tokio::runtime::Runtime>(&RT)
                .iter(|| async move {
                    let sleep_seconds = "0";
                    let mut futures = Vec::new();
                    for _ in 0..ELEMENTS_PER_ITERATION {
                        futures.push(Command::new(exe).arg(sleep_seconds).status());
                    }
                    assert!(futures_util::future::join_all(futures)
                        .await
                        .into_iter()
                        .collect::<Result<Vec<_>, _>>()
                        .unwrap()
                        .into_iter()
                        .all(|status| status.success()));
                });
        });
    }
    group.finish();
}

criterion_group! {
  name = sleep;
  config = Criterion::default();
  targets = benchmark_sleep
}

criterion_group! {
  name = sleep_process;
  config = Criterion::default();
  targets = benchmark_sleep_process
}
criterion_main!(sleep, sleep_process);
