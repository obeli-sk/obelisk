use concepts::{
    ExecutionId,
    prefixed_ulid::{DeploymentId, RunId},
};
use serde_json::{Value, json};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

#[tokio::test]
async fn execution_results_and_follow_logs_keep_stdout_structured() {
    let server = MockServer::start().await;
    let execution_id = ExecutionId::generate().to_string();
    let execution_path = format!("/v1/executions/{execution_id}");
    let result = json!({"ok": {"items": [1, 2], "message": "done"}});
    let status = json!({
        "execution_id": execution_id,
        "ffqn": "testing:output/activity.run",
        "pending_state": {
            "status": "finished", "version": 2,
            "finished_at": "2026-11-03T00:00:00Z", "result_kind": "ok"
        },
        "created_at": "2026-11-03T00:00:00Z",
        "first_scheduled_at": "2026-11-03T00:00:00Z",
        "component_digest": format!("sha256:{}", "0".repeat(64)),
        "component_type": "activity",
        "deployment_id": DeploymentId::generate().to_string()
    });
    let log = json!({
        "cursor": "next", "run_id": RunId::generate().to_string(),
        "execution_id": execution_id, "type": "log",
        "created_at": "2026-11-03T00:00:00Z", "level": "info", "message": "human log"
    });
    Mock::given(method("PUT"))
        .and(path(&execution_path))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({"ok": execution_id})))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(&execution_path))
        .respond_with(ResponseTemplate::new(200).set_body_json(&result))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("{execution_path}/status")))
        .respond_with(move |request: &wiremock::Request| {
            if request.headers.get("accept").unwrap() == "text/event-stream" {
                ResponseTemplate::new(200)
                    .insert_header("content-type", "text/event-stream")
                    .set_body_string(format!("data: {status}\n\n"))
            } else {
                ResponseTemplate::new(200).set_body_json(&status)
            }
        })
        .mount(&server)
        .await;
    let log_response = log.clone();
    Mock::given(method("GET"))
        .and(path(format!("{execution_path}/logs")))
        .respond_with(move |request: &wiremock::Request| {
            let logs = if request.url.query_pairs().any(|(key, _)| key == "cursor") {
                json!([])
            } else {
                json!([log_response])
            };
            ResponseTemplate::new(200).set_body_json(logs)
        })
        .mount(&server)
        .await;

    let run = async |args: &[&str]| {
        tokio::time::timeout(
            std::time::Duration::from_secs(30),
            tokio::process::Command::new(env!("CARGO_BIN_EXE_obelisk"))
                .args(["execution", "--api-url", &server.uri()])
                .args(args)
                .env_remove("OBELISK_API_TOKEN")
                .output(),
        )
        .await
        .unwrap()
        .unwrap()
    };
    for follow in ["-f", "--follow-logs"] {
        for json_mode in [false, true] {
            let mut args = vec![
                "submit",
                "--execution-id",
                &execution_id,
                follow,
                "testing:output/activity.run",
            ];
            if json_mode {
                args.push("--json");
            }
            let output = run(&args).await;
            let stdout = String::from_utf8(output.stdout).unwrap();
            let stderr = String::from_utf8(output.stderr).unwrap();
            assert!(output.status.success(), "{stderr}");
            if json_mode {
                let rows = stdout
                    .lines()
                    .map(|line| serde_json::from_str::<Value>(line).unwrap())
                    .collect::<Vec<_>>();
                let mut expected = vec![json!({"ok": execution_id})];
                if follow == "--follow-logs" {
                    expected.push(log.clone());
                }
                expected.push(result.clone());
                assert_eq!(rows, expected);
                assert!(stderr.is_empty(), "{stderr}");
            } else {
                assert_eq!(
                    stdout,
                    format!(
                        "{execution_id}\n{}\n",
                        serde_json::to_string_pretty(&result).unwrap()
                    )
                );
                assert!(stderr.contains("Execution finished: OK"), "{stderr}");
                if follow == "--follow-logs" {
                    assert!(stderr.contains("human log"), "{stderr}");
                } else {
                    assert!(stderr.contains("Finished: OK"), "{stderr}");
                }
            }
        }
    }
    for result in [
        result,
        json!({"err": {"message": "failed"}}),
        json!({"ok": null}),
    ] {
        Mock::given(method("GET"))
            .and(path(&execution_path))
            .respond_with(ResponseTemplate::new(200).set_body_json(&result))
            .with_priority(1)
            .up_to_n_times(1)
            .mount(&server)
            .await;
        let output = run(&["result", &execution_id]).await;
        assert_eq!(output.status.success(), result.get("ok").is_some());
        assert_eq!(
            String::from_utf8(output.stdout).unwrap(),
            format!("{}\n", serde_json::to_string_pretty(&result).unwrap())
        );
        assert!(
            String::from_utf8(output.stderr)
                .unwrap()
                .contains("Execution finished:")
        );
    }
}
