use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use lazy_static::lazy_static;
use runtime::{
    activity::ActivityConfig, event_history::EventHistory, runtime::Runtime,
    workflow::AsyncActivityBehavior, workflow::WorkflowConfig, workflow_id::WorkflowId,
    FunctionFqn,
};
use std::sync::Arc;

use wiremock::{
    matchers::{method, path},
    Mock, MockServer, ResponseTemplate,
};

lazy_static! {
    static ref RT: tokio::runtime::Runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .expect("Should create a tokio runtime");
}

fn workflow() -> Arc<Runtime> {
    RT.block_on(async {
        let mut runtime = Runtime::default();
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
        Arc::new(runtime)
    })
}

fn benchmark_http(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("throughput");
    group.throughput(Throughput::Elements(1));
    group.bench_function("http", |b| {
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
        let runtime = workflow();

        b.to_async::<&tokio::runtime::Runtime>(&RT).iter(|| {
            let params = vec![wasmtime::component::Val::U16(port)];
            let mut event_history = EventHistory::default();
            let fqn = fqn.clone();
            let runtime = runtime.clone();
            async move {
                runtime
                    .schedule_workflow(&WorkflowId::generate(), &mut event_history, &fqn, &params)
                    .await
                    .unwrap()
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
