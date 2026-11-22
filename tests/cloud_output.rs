use axum::{Json, Router, extract::Query, routing::get};
use serde_json::{Value, json};

#[tokio::test]
async fn cloud_output() {
    let app = json!({
        "app_name":"demo", "status":"running", "hostname":"demo.example.com"
    });
    let router = Router::new()
        .route(
            "/v1/apps",
            get({
                let app = app.clone();
                move || {
                    let app = app.clone();
                    async move { Json(json!({"apps":[app]})) }
                }
            }),
        )
        .route(
            "/v1/apps/demo/status",
            get(|| async {
                Json(json!({
                    "app":{"app_name":"demo", "status":"stopped", "desired_state":"running", "endpoints":{
                        "api":"https://api--demo.example.com", "webui":"https://webui--demo.example.com",
                        "internal":"https://internal--demo.example.com"}},
                    "machine_state":"started", "api_ready":false
                }))
            }),
        )
        .route(
            "/v1/apps/demo/drafts",
            get(|| async { Json(json!({"drafts":[], "applied_revision":null})) }),
        )
        .route(
            "/v1/apps/demo/events",
            get(|Query(query): Query<std::collections::HashMap<String, String>>| async move {
                Json(json!({"events":[{
                    "event_id":1, "created_at":0, "level":"warn", "code":"machine.health.critical",
                    "message":format!("limit {}", query["limit"]), "details":null
                }]}))
            }),
        )
        .route(
            "/v1/apps/demo/logs",
            get(|Query(query): Query<std::collections::HashMap<String, String>>| async move {
                Json(json!({"logs":[{
                    "timestamp":"2026-11-22T15:28:01.548908094Z", "level":"info", "machine_id":"m1",
                    "region":"ams", "source":"app",
                    "message":format!("limit {} after {}", query["limit"], query.get("after").map_or("-", String::as_str))
                }], "next":"1795361281548908094"}))
            }),
        )
        .route("/v1/tokens", get(|| async { Json(json!({"tokens":[]})) }).post(|Json(body): Json<Value>| async move {
            assert!(body.get("hash").is_none());
            assert!(body["global"] == true || !body["apps"].as_array().unwrap().is_empty());
            Json(json!({"token_id":"T_ID", "token":"obk_secret", "name":body["name"], "creation":body, "revoked":false, "expires_at":null}))
        }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let directory = tempfile::tempdir().unwrap();
    std::fs::write(directory.path().join("app.toml"), "invalid TOML [").unwrap();
    let run = async |args: &[&str]| {
        let output = tokio::process::Command::new(env!("CARGO_BIN_EXE_obelisk"))
            .arg("cloud")
            .args(args)
            .current_dir(directory.path())
            .env("OBELISK_CLOUD_API_TOKEN", "test-token")
            .env("OBELISK_CLOUD_API_URL", &origin)
            .env("OBELISK_CLOUD_APPS_DOMAIN", "example.com")
            .env(
                "OBELISK_APP_NAME",
                if args[0] == "token" {
                    "invalid/name"
                } else {
                    "demo"
                },
            )
            .env("OBELISK_APP_API_URL", "https://ignored.example.com")
            .output()
            .await
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        (
            String::from_utf8(output.stdout).unwrap(),
            String::from_utf8(output.stderr).unwrap(),
        )
    };
    let status = run(&["app", "status"]).await.1;
    for line in [
        "API: https://api--demo.example.com (unavailable)",
        "Web UI: https://webui--demo.example.com",
        "Internal server: https://internal--demo.example.com",
    ] {
        assert!(status.contains(line), "{status}");
    }
    assert!(!status.contains("External server"), "{status}");
    assert!(status.contains("Intended state: running"), "{status}");
    assert!(
        status.contains("VM is started although the app is stopped."),
        "{status}"
    );
    let events = run(&["app", "events", "-n", "7"]).await.1;
    assert_eq!(
        events,
        "1970-01-01 00:00:00 UTC  warn   limit 7  (machine.health.critical)\n"
    );
    assert!(run(&["app", "events"]).await.1.contains("limit 5"));
    assert_eq!(
        run(&["app", "logs"]).await.1,
        "2026-11-22T15:28:01.548908094Z  m1  app     limit 100 after -\n"
    );
    assert_eq!(
        run(&["app", "logs", "-n", "1", "--since", "2026-11-22T15:28:00Z"])
            .await
            .1,
        "2026-11-22T15:28:01.548908094Z  m1  app     limit 1 after 1795361280000000000\n\
         More lines may follow; continue with `--since 2026-11-22T15:28:01.548908094Z`.\n"
    );
    for command in [vec!["app", "list"], vec!["token", "list"]] {
        let (stdout, text) = run(&command).await;
        assert!(stdout.is_empty() && !text.is_empty(), "{stdout}");
        let mut json_command = command.clone();
        json_command.push("--json");
        let parsed: Value = serde_json::from_str(&run(&json_command).await.0).unwrap();
        assert_eq!(
            parsed,
            match command[..] {
                ["app", "list"] => json!({"apps":[app]}),
                _ => json!({"tokens":[]}),
            }
        );
    }
    let account = run(&[
        "token",
        "new",
        "--name",
        "laptop",
        "--token-file",
        "account.token",
    ])
    .await
    .1;
    assert!(account.contains("scope: account-wide"), "{account}");
    let scoped = run(&[
        "token",
        "new",
        "--name",
        "ci",
        "--token-file",
        "ci.token",
        "--scope",
        "demo",
        "--scope",
        "worker",
    ])
    .await
    .1;
    assert!(scoped.contains("scope: demo, worker"), "{scoped}");
    assert_eq!(
        std::fs::read_to_string(directory.path().join("ci.token")).unwrap(),
        "obk_secret"
    );
    server.abort();
}
