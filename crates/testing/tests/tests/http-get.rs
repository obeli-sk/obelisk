use runtime::{
    activity::ActivityConfig,
    event_history::{EventHistory, SupportedFunctionResult},
    runtime::Runtime,
    workflow::WorkflowConfig,
    workflow_id::WorkflowId,
    FunctionFqn,
};
use std::time::Instant;
use tracing::info;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};
use wasmtime::component::Val;
use wiremock::{
    matchers::{method, path},
    Mock, MockServer, ResponseTemplate,
};

#[tokio::test]
async fn test() -> Result<(), anyhow::Error> {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

    let mut runtime = Runtime::default();
    runtime
        .add_activity(
            test_programs_http_get_activity_builder::TEST_PROGRAMS_HTTP_GET_ACTIVITY.to_string(),
            &ActivityConfig::default(),
        )
        .await?;
    runtime
        .add_workflow_definition(
            test_programs_http_get_workflow_builder::TEST_PROGRAMS_HTTP_GET_WORKFLOW.to_string(),
            &WorkflowConfig::default(),
        )
        .await?;
    let mut event_history = EventHistory::default();
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

    let params = vec![wasmtime::component::Val::U16(server.address().port())];
    let res = runtime
        .schedule_workflow(
            &WorkflowId::generate(),
            &mut event_history,
            &FunctionFqn::new("testing:http-workflow/workflow", "execute"),
            &params,
        )
        .await;
    info!("Finished: in {duration:?}", duration = timer.elapsed());
    assert_eq!(
        res.unwrap(),
        SupportedFunctionResult::Single(Val::String(BODY.into()))
    );
    assert_eq!(event_history.successful_activities(), 1);
    server.verify().await;
    Ok(())
}
