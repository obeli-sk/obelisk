use super::*;

const GREET_DEPLOYMENT: &str = r#"[[activity_js]]
name = "test_greet_activity"
ffqn = "testing:integration/activity-greet.greet"
content = '''
export default function greet(name) {
    console.info("Greeting " + name);
    return "Hello, " + name + "!";
}
'''
params = [
  { name = "name", type = "string" },
]
return_type = "result<string, string>"
"#;

async fn start(ip: String) -> TestServer {
    TestServer::start_inline_deployment(ip, "", GREET_DEPLOYMENT, &[]).await
}

async fn submit_greet(server: &TestServer, exec_id: &str) {
    let resp = server
        .submit_follow_with_id(
            exec_id,
            "testing:integration/activity-greet.greet",
            vec![json!("World")],
        )
        .await;
    assert_eq!(resp.status().as_u16(), 201);
    // Consume the streamed body to wait for the execution to finish.
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body, json!({ "ok": "Hello, World!" }));
}

#[tokio::test]
async fn events() {
    let server = start(test_addr!(5)).await;
    let exec_id = server.generate_execution_id().await;
    submit_greet(&server, &exec_id).await;

    let events = server.get_events(&exec_id).await;
    let events = sanitize_json(&events);
    insta::assert_json_snapshot!("greet_activity_events", events);
    server.shutdown().await;
}

#[tokio::test]
async fn logs() {
    let server = start(test_addr!(6)).await;
    let exec_id = server.generate_execution_id().await;
    submit_greet(&server, &exec_id).await;

    let logs = loop {
        let logs = server.get_logs(&exec_id, 1).await;
        if logs.as_array().expect("logs must be an array").len() == 1 {
            break logs;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    let logs = sanitize_json(&logs);
    insta::assert_json_snapshot!("greet_activity_logs", logs);
    server.shutdown().await;
}

#[tokio::test]
async fn status() {
    let server = start(test_addr!(7)).await;
    let exec_id = server.generate_execution_id().await;
    submit_greet(&server, &exec_id).await;

    let status = server.get_status(&exec_id).await;
    let status = sanitize_json(&status);
    insta::assert_json_snapshot!("greet_activity_status", status);
    server.shutdown().await;
}
