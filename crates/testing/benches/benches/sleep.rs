use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use lazy_static::lazy_static;
use runtime::{
    activity::ACTIVITY_CONFIG_HOT,
    database::Database,
    event_history::EventHistory,
    runtime::{Runtime, RuntimeBuilder},
    workflow::WORKFLOW_CONFIG_HOT,
    workflow_id::WorkflowId,
    FunctionFqn,
};
use std::sync::{atomic::Ordering, Arc, Once};
use tokio::sync::Mutex;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use wasmtime::component::Val;

const DB_BUFFER_CAPACITY: usize = 100;
const TASKS_PER_RUNTIME: usize = 20;
const ELEMENTS_PER_ITERATION: u64 = 100;
const SLEEP_MILLIS: u64 = 10;

lazy_static! {
    static ref RT: tokio::runtime::Runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_time()
        .build()
        .expect("Should create a tokio runtime");
}

static INIT: Once = Once::new();
fn set_up() {
    INIT.call_once(|| {
        tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .init();
    });
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
            .add_workflow_definition(
                test_programs_sleep_workflow_builder::TEST_PROGRAMS_SLEEP_WORKFLOW.to_string(),
                &WORKFLOW_CONFIG_HOT,
            )
            .await
            .unwrap();
        runtime.build()
    })
}

static COUNTER: std::sync::atomic::AtomicU16 = std::sync::atomic::AtomicU16::new(0);

fn benchmark_sleep(criterion: &mut Criterion) {
    set_up();
    let database = Database::new(DB_BUFFER_CAPACITY, DB_BUFFER_CAPACITY);
    let runtime = setup_runtime();
    let _abort_handles: Vec<_> = RT.block_on(async {
        (0..TASKS_PER_RUNTIME)
            .map(|_| runtime.spawn(&database))
            .collect()
    });

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
            })
        });
    }
    group.finish();
}

criterion_group! {
  name = sleep;
  config = Criterion::default();
  targets = benchmark_sleep
}
criterion_main!(sleep,);
