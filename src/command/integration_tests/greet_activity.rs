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

async fn start(ip: String, runtime: JsRuntime) -> TestServer {
    TestServer::start_inline_deployment_with_js_runtime(
        ip,
        "",
        GREET_DEPLOYMENT,
        &[],
        runtime.mode(),
    )
    .await
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

#[rstest::rstest]
#[case::boa_wasm(JsRuntime::BoaWasm)]
#[case::v8(JsRuntime::V8)]
#[tokio::test]
async fn events(#[case] runtime: JsRuntime) {
    let ip = match runtime {
        JsRuntime::BoaWasm => test_addr!(5),
        JsRuntime::V8 => test_addr!(160),
    };
    let server = start(ip, runtime).await;
    let exec_id = server.generate_execution_id().await;
    submit_greet(&server, &exec_id).await;

    let events = server.get_events(&exec_id).await;
    let events = sanitize_json(&events);
    insta::assert_json_snapshot!("greet_activity_events", events);
    server.shutdown().await;
}

#[rstest::rstest]
#[case::boa_wasm(JsRuntime::BoaWasm)]
#[case::v8(JsRuntime::V8)]
#[tokio::test]
async fn logs(#[case] runtime: JsRuntime) {
    let ip = match runtime {
        JsRuntime::BoaWasm => test_addr!(6),
        JsRuntime::V8 => test_addr!(161),
    };
    let server = start(ip, runtime).await;
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

#[rstest::rstest]
#[case::boa_wasm(JsRuntime::BoaWasm)]
#[case::v8(JsRuntime::V8)]
#[tokio::test]
async fn status(#[case] runtime: JsRuntime) {
    let ip = match runtime {
        JsRuntime::BoaWasm => test_addr!(7),
        JsRuntime::V8 => test_addr!(162),
    };
    let server = start(ip, runtime).await;
    let exec_id = server.generate_execution_id().await;
    submit_greet(&server, &exec_id).await;

    let status = server.get_status(&exec_id).await;
    let status = sanitize_json(&status);
    insta::assert_json_snapshot!("greet_activity_status", status);
    server.shutdown().await;
}
