use runtime::{
    activity::ActivityConfig, database::Database, event_history::EventHistory,
    runtime::RuntimeBuilder, workflow::WorkflowConfig, workflow_id::WorkflowId, FunctionFqn,
    SupportedFunctionResult,
};
use std::{
    sync::{Arc, Once},
    time::Instant,
};
use tokio::sync::Mutex;
use tracing::info;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};
use wasmtime::component::Val;
use wiremock::{
    matchers::{method, path},
    Mock, MockServer, ResponseTemplate,
};

static INIT: Once = Once::new();
fn set_up() {
    INIT.call_once(|| {
        tracing_subscriber::registry()
            .with(fmt::layer())
            .with(EnvFilter::from_default_env())
            .init();
    });
}

#[tokio::test]
async fn test() -> Result<(), anyhow::Error> {
    set_up();
    let database = Database::new(100, 100);
    let mut runtime = RuntimeBuilder::default();
    runtime
        .add_activity(
            test_programs_http_get_activity_builder::TEST_PROGRAMS_HTTP_GET_ACTIVITY.to_string(),
            &ActivityConfig::default(),
        )
        .await?;
    let runtime = runtime
        .build(
            test_programs_http_get_workflow_builder::TEST_PROGRAMS_HTTP_GET_WORKFLOW.to_string(),
            &WorkflowConfig::default(),
        )
        .await?;
    let _abort_handle = runtime.spawn(&database);
    let event_history = Arc::new(Mutex::new(EventHistory::default()));
    let timer = Instant::now();

    const BODY: &str = "ok";
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(200).set_body_string(BODY))
        .expect(1)
        .mount(&server)
        .await;
    info!("started mock server on {}", server.address());

    let params = Arc::new(vec![wasmtime::component::Val::U16(server.address().port())]);
    let res = database
        .workflow_scheduler()
        .schedule_workflow(
            WorkflowId::generate(),
            event_history.clone(),
            FunctionFqn::new("testing:http-workflow/workflow", "execute"),
            params,
        )
        .await;
    info!("Finished: in {duration:?}", duration = timer.elapsed());
    assert_eq!(
        res.unwrap(),
        SupportedFunctionResult::Single(Val::String(BODY.into()))
    );
    assert_eq!(event_history.lock().await.successful_activities(), 1);
    server.verify().await;
    Ok(())
}
