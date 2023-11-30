use std::{sync::Arc, time::Instant};

use runtime::{activity::Activities, event_history::EventHistory, workflow::Workflow};
use wiremock::{
    matchers::{method, path},
    Mock, MockServer, ResponseTemplate,
};

#[tokio::test]
async fn test() -> Result<(), anyhow::Error> {
    let activities = Arc::new(dbg!(
        Activities::new(test_programs_builder::TEST_PROGRAMS_HTTP_GET_ACTIVITY.to_string()).await?
    ));
    let workflow = Workflow::new(
        test_programs_builder::TEST_PROGRAMS_HTTP_GET_WORKFLOW.to_string(),
        activities.clone(),
    )
    .await?;
    let mut event_history = EventHistory::new();
    let timer = Instant::now();

    // TODO: use random port
    let mock_listener = std::net::TcpListener::bind("127.0.0.1:8080").unwrap();
    const BODY: &str = "ok";
    let server = MockServer::builder().listener(mock_listener).start().await;
    Mock::given(method("GET"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(200).set_body_string(BODY))
        .expect(1)
        .mount(&server)
        .await;
    println!("started mock server on {}", server.address());

    let res = workflow
        .execute_all(
            &mut event_history,
            Some("testing:http-workflow/workflow"),
            "execute",
        )
        .await;
    println!("Finished: in {duration:?}", duration = timer.elapsed());
    assert_eq!(event_history.len(), 1);
    assert_eq!(res.unwrap(), BODY);
    server.verify().await;
    Ok(())
}
