use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use lazy_static::lazy_static;
use runtime::{
    activity::{ActivityConfig, ACTIVITY_CONFIG_COLD, ACTIVITY_CONFIG_HOT},
    database::Database,
    event_history::EventHistory,
    runtime::{Runtime, RuntimeBuilder},
    workflow::{WorkflowConfig, WORKFLOW_CONFIG_COLD, WORKFLOW_CONFIG_HOT},
    workflow_id::WorkflowId,
    FunctionFqn,
};
use std::sync::Arc;
use tokio::sync::Mutex;
use wasmtime::component::Val;

lazy_static! {
    static ref RT: tokio::runtime::Runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("Should create a tokio runtime");
}

fn benchmark_engine_creation(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("throughput");
    group.throughput(Throughput::Elements(2)); // Workflow and Activity engine are created.
    group.bench_function("engine", |b| b.iter(RuntimeBuilder::default));
}

fn noop_workflow(
    mut runtime: RuntimeBuilder,
    activity_config: &ActivityConfig,
    workflow_config: &WorkflowConfig,
) -> Runtime {
    RT.block_on(async move {
        runtime
            .add_activity(
                test_programs_noop_activity_builder::TEST_PROGRAMS_NOOP_ACTIVITY.to_string(),
                activity_config,
            )
            .await
            .unwrap();
        runtime
            .add_workflow_definition(
                test_programs_noop_workflow_builder::TEST_PROGRAMS_NOOP_WORKFLOW.to_string(),
                workflow_config,
            )
            .await
            .unwrap();
        runtime.build()
    })
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
        ("noopw", 1, ACTIVITY_CONFIG_COLD, WORKFLOW_CONFIG_COLD), /* no interruptions */
        ("noopw", 100, ACTIVITY_CONFIG_COLD, WORKFLOW_CONFIG_COLD), /* no interruptions */
        ("noopa", 1, ACTIVITY_CONFIG_COLD, WORKFLOW_CONFIG_COLD),
        ("noopa", 1, ACTIVITY_CONFIG_COLD, WORKFLOW_CONFIG_HOT),
        ("noopa", 100, ACTIVITY_CONFIG_COLD, WORKFLOW_CONFIG_COLD),
        ("noopa", 100, ACTIVITY_CONFIG_COLD, WORKFLOW_CONFIG_HOT),
        ("noopa", 100, ACTIVITY_CONFIG_HOT, WORKFLOW_CONFIG_HOT),
        ("noopha", 1, ACTIVITY_CONFIG_COLD, WORKFLOW_CONFIG_COLD),
        ("noopha", 1, ACTIVITY_CONFIG_COLD, WORKFLOW_CONFIG_HOT),
        ("noopha", 100, ACTIVITY_CONFIG_COLD, WORKFLOW_CONFIG_COLD),
        ("noopha", 100, ACTIVITY_CONFIG_COLD, WORKFLOW_CONFIG_HOT),
    ];
    let runtime_builder = Mutex::new(RuntimeBuilder::default());
    for (workflow_function, iterations, activity_config, workflow_config) in functions {
        criterion.bench_function(
            &format!(
                "{workflow_function}*{iterations}{hot_or_cold}",
                hot_or_cold = hot_or_cold(&activity_config, &workflow_config)
            ),
            |b| {
                let fqn = FunctionFqn::new("testing:types-workflow/workflow", workflow_function);
                let database = Database::new(100, 100);
                let runtime_builder = runtime_builder.try_lock().unwrap().clone();
                let runtime = noop_workflow(runtime_builder, &activity_config, &workflow_config);
                b.to_async::<&tokio::runtime::Runtime>(&RT).iter(|| {
                    let abort_handle = runtime.spawn(&database);
                    let workflow_scheduler = database.workflow_scheduler();
                    let params = Arc::new(vec![Val::U32(iterations)]);
                    let event_history = Arc::new(Mutex::new(EventHistory::default()));
                    let fqn = fqn.clone();

                    async move {
                        workflow_scheduler
                            .schedule_workflow(WorkflowId::generate(), event_history, fqn, params)
                            .await
                            .unwrap();
                        abort_handle.abort(); // TODO: needed?
                    }
                })
            },
        );
    }
}

criterion_group! {
  name = engine_creation;
  config = Criterion::default();
  targets = benchmark_engine_creation
}

criterion_group! {
  name = noop_benches;
  config = Criterion::default();
  targets = benchmark_noop_functions
}
criterion_main!(engine_creation, noop_benches,);
