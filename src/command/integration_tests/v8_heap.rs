use super::*;

#[tokio::test]
async fn v8_heap_exhaustion_keeps_server_available() {
    const CHILD_ENV: &str = "OBELISK_TEST_V8_HEAP_CHILD";
    if std::env::var_os(CHILD_ENV).is_none() {
        let test_name = format!(
            "{}::v8_heap_exhaustion_keeps_server_available",
            module_path!().split_once("::").unwrap().1
        );
        let mut child = tokio::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", &test_name, "--nocapture"])
            .env(CHILD_ENV, "1")
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        if let Ok(status) = tokio::time::timeout(Duration::from_secs(120), child.wait()).await {
            assert!(
                status.unwrap().success(),
                "heap regression subprocess failed"
            );
        } else {
            child.kill().await.unwrap();
            panic!("heap regression subprocess timed out");
        }
    } else {
        run_heap_exhaustion().await;
    }
}

async fn run_heap_exhaustion() {
    let allocation = r#"
function exhaust() {
    const arrays = [];
    for (let i = 0; i < 300; i++) arrays.push(new Array(200000).fill(i));
    return "survived";
}
export default function run(mode) {
    if (mode === "body") return exhaust();
    if (mode === "result") return { get value() { return exhaust(); } };
    return "ok";
}
"#;
    let mut deployment = String::new();
    for section in ["activity_js", "workflow_js"] {
        let interface = section.strip_suffix("_js").unwrap();
        writeln!(
            deployment,
            r#"[[{section}]]
ffqn = "testing:heap/{interface}.run"
content = '''{allocation}'''
params = [{{ name = "mode", type = "string" }}]
return_type = "result<string, string>"
"#
        )
        .unwrap();
        if section == "activity_js" {
            deployment.push_str("max_retries = 0\n");
        }
    }
    deployment.push_str(
        r#"
[[webhook_endpoint_js]]
name = "heap_webhook"
routes = ["/heap"]
content = '''
function exhaust() {
    const arrays = [];
    for (let i = 0; i < 300; i++) arrays.push(new Array(200000).fill(i));
    return "survived";
}
export default function handle(request) {
    if (request.url.includes("body")) return new Response(exhaust());
    const response = new Response("ok");
    if (request.url.includes("result")) response.text = () => exhaust();
    return response;
}
'''
"#,
    );
    let server = TestServer::start_inline_deployment_with_js_runtime(
        test_addr!(40_201),
        r"
[limits.activities.v8]
count = 1
memory.mib = 32
[limits.workflows.v8]
count = 1
memory.mib = 32
[limits.webhooks.v8]
count = 1
memory.mib = 32
",
        &deployment,
        &[],
        JsRuntimeMode::V8,
    )
    .await;
    for mode in ["body", "result", "body"] {
        for interface in ["activity", "workflow"] {
            let ffqn = format!("testing:heap/{interface}.run");
            let failure: Value = server
                .submit_follow(&ffqn, vec![json!(mode)])
                .await
                .json()
                .await
                .unwrap();
            assert!(failure.get("execution_failed").is_some(), "{failure}");
            assert!(
                failure
                    .to_string()
                    .contains("JavaScript heap limit exceeded"),
                "{failure}"
            );
            let normal: Value = server
                .submit_follow(&ffqn, vec![json!("normal")])
                .await
                .json()
                .await
                .unwrap();
            assert_eq!(normal, json!({ "ok": "ok" }));
        }
        let failed = server
            .client
            .get(format!("{}/heap?{mode}", server.webhook_base_url))
            .send()
            .await
            .unwrap();
        assert_eq!(failed.status(), reqwest::StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(failed.text().await.unwrap(), "Component Error");
        let normal = server
            .client
            .get(format!("{}/heap", server.webhook_base_url))
            .send()
            .await
            .unwrap();
        assert!(normal.status().is_success());
        assert_eq!(normal.text().await.unwrap(), "ok");
    }
    server.shutdown_with_timeout(Duration::from_secs(10)).await;
}
