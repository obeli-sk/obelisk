use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use lazy_static::lazy_static;
use runtime::runtime::RuntimeBuilder;
use runtime::{
    activity::ActivityConfig, database::Database, event_history::EventHistory, runtime::Runtime,
    workflow::AsyncActivityBehavior, workflow::WorkflowConfig, workflow_id::WorkflowId,
    FunctionFqn,
};
use std::sync::Arc;
use std::sync::{atomic::Ordering, Once};
use tokio::sync::Mutex;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use wiremock::{
    matchers::{method, path},
    Mock, MockServer, ResponseTemplate,
};

lazy_static! {
    static ref RT: tokio::runtime::Runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_io()
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

fn workflow() -> Runtime {
    RT.block_on(async {
        let mut runtime = RuntimeBuilder::default();
        runtime
            .add_activity(
                test_programs_http_get_activity_builder::TEST_PROGRAMS_HTTP_GET_ACTIVITY
                    .to_string(),
                &ActivityConfig::default(),
            )
            .await
            .unwrap();
        runtime
            .add_workflow_definition(
                test_programs_http_get_workflow_builder::TEST_PROGRAMS_HTTP_GET_WORKFLOW
                    .to_string(),
                &WorkflowConfig {
                    async_activity_behavior: AsyncActivityBehavior::KeepWaiting,
                },
            )
            .await
            .unwrap();
        runtime.build()
    })
}

static COUNTER: std::sync::atomic::AtomicU16 = std::sync::atomic::AtomicU16::new(0);

fn benchmark_http(criterion: &mut Criterion) {
    set_up();
    let database = Database::new(100, 100);
    let runtime = workflow();
    RT.block_on(async {
        let worker_threads = 20;
        println!("Runtime is using {} workers", worker_threads);
        for _ in 0..worker_threads {
            runtime.spawn(&database);
        }
    });
    let port = RT.block_on(async {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/"))
            .respond_with(ResponseTemplate::new(200).set_body_string(""))
            .mount(&server)
            .await;
        server.address().port()
    });
    let fqn = FunctionFqn::new("testing:http-workflow/workflow", "execute");

    const ELEMENTS: u64 = 10;
    let mut group = criterion.benchmark_group("throughput");
    group.throughput(Throughput::Elements(ELEMENTS));
    group.bench_function("http", |b| {
        b.to_async::<&tokio::runtime::Runtime>(&RT).iter(|| {
            let params = Arc::new(vec![wasmtime::component::Val::U16(port)]);
            let fqn = fqn.clone();
            let workflow_scheduler = database.workflow_scheduler();
            async move {
                let mut futures = Vec::new();
                for _ in 0..ELEMENTS {
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
    group.finish();
}

criterion_group! {
  name = http_benches;
  config = Criterion::default();
  targets = benchmark_http
}
criterion_main!(http_benches);
