use concepts::workflow_id::WorkflowId;
use concepts::FunctionFqnStr;
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use lazy_static::lazy_static;
use runtime::activity::ACTIVITY_CONFIG_HOT;
use runtime::runtime::RuntimeBuilder;
use runtime::workflow::WORKFLOW_CONFIG_HOT;
use runtime::{database::Database, event_history::EventHistory, runtime::Runtime};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::sync::Mutex;
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

fn setup_runtime() -> Runtime {
    RT.block_on(async {
        let mut runtime = RuntimeBuilder::default();
        runtime
            .add_activity(
                test_programs_http_get_activity_builder::TEST_PROGRAMS_HTTP_GET_ACTIVITY
                    .to_string(),
                &ACTIVITY_CONFIG_HOT,
            )
            .await
            .unwrap();
        runtime
            .build(
                test_programs_http_get_workflow_builder::TEST_PROGRAMS_HTTP_GET_WORKFLOW
                    .to_string(),
                &WORKFLOW_CONFIG_HOT,
            )
            .await
            .unwrap()
    })
}

static COUNTER: std::sync::atomic::AtomicU16 = std::sync::atomic::AtomicU16::new(0);

const ELEMENTS: u64 = 10;

fn benchmark_activity_client(criterion: &mut Criterion) {
    let database = Database::new(100, 100);
    let runtime = setup_runtime();
    let _abort_handle = RT.block_on(async { runtime.spawn(&database) });
    let port = RT.block_on(async {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/"))
            .respond_with(ResponseTemplate::new(200).set_body_string(""))
            .mount(&server)
            .await;
        server.address().port()
    });
    let fqn = FunctionFqnStr::new("testing:http-workflow/workflow", "execute");
    let mut group = criterion.benchmark_group("benchmark_activity_client");
    group.throughput(Throughput::Elements(ELEMENTS));
    group.bench_function("http", |b| {
        b.to_async::<&tokio::runtime::Runtime>(&RT).iter(|| {
            let params = Arc::new(vec![wasmtime::component::Val::U16(port)]);
            let fqn = fqn.to_owned();
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
        });
    });
    group.finish();
}

fn benchmark_hyper_client(criterion: &mut Criterion) {
    use http_body_util::Empty;
    use hyper::body::Bytes;
    use hyper::Request;
    use hyper_util::rt::TokioIo;
    use tokio::net::TcpStream;

    async fn fetch_url(
        url: &hyper::Uri,
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let host = url.host().expect("uri has no host");
        let port = url.port_u16().unwrap_or(80);
        let addr = format!("{}:{}", host, port);
        let stream = TcpStream::connect(addr).await?;
        let io = TokioIo::new(stream);
        let (mut sender, conn) = hyper::client::conn::http1::handshake(io).await?;
        tokio::task::spawn(async move {
            if let Err(err) = conn.await {
                println!("Connection failed: {:?}", err);
            }
        });
        let authority = url.authority().unwrap().clone();
        let path = url.path();
        let req = Request::builder()
            .uri(path)
            .header(hyper::header::HOST, authority.as_str())
            .body(Empty::<Bytes>::new())?;
        assert!(sender.send_request(req).await?.status().is_success());
        Ok(())
    }
    let port = RT.block_on(async {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/"))
            .respond_with(ResponseTemplate::new(200).set_body_string(""))
            .mount(&server)
            .await;
        server.address().port()
    });
    let url = format!("http://localhost:{port}/").parse().unwrap();
    let mut group = criterion.benchmark_group("benchmark_hyper_client");
    group.throughput(Throughput::Elements(ELEMENTS));
    group.bench_function("http", |b| {
        b.to_async::<&tokio::runtime::Runtime>(&RT).iter(|| async {
            let mut futures = Vec::new();
            for _ in 0..ELEMENTS {
                futures.push(fetch_url(&url));
            }
            futures_util::future::join_all(futures)
                .await
                .into_iter()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
        });
    });
    group.finish();
}

criterion_group! {
  name = benchmark_activity_client_group;
  config = Criterion::default();
  targets = benchmark_activity_client
}
criterion_group! {
  name = benchmark_hyper_client_group;
  config = Criterion::default();
  targets = benchmark_hyper_client
}
criterion_main!(
    benchmark_activity_client_group,
    benchmark_hyper_client_group
);
