use std::{sync::Arc, time::Instant};

use runtime::{
    activity::Activities,
    event_history::{EventHistory, SupportedFunctionResult},
    workflow::Workflow,
};
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

    let activities = Arc::new(
        Activities::new(test_programs_builder::TEST_PROGRAMS_HTTP_GET_ACTIVITY.to_string()).await?,
    );
    let workflow = Workflow::new(
        test_programs_builder::TEST_PROGRAMS_HTTP_GET_WORKFLOW.to_string(),
        activities.clone(),
    )
    .await?;
    let mut event_history = EventHistory::new();
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
    let res = workflow
        .execute_all(
            &mut event_history,
            "testing:http-workflow/workflow",
            "execute",
            &params,
        )
        .await;
    info!("Finished: in {duration:?}", duration = timer.elapsed());
    assert_eq!(
        res.unwrap(),
        SupportedFunctionResult::Single(Val::String(BODY.into()))
    );
    assert_eq!(event_history.len(), 1);
    server.verify().await;
    Ok(())
}
