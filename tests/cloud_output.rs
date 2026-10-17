use axum::{Json, Router, routing::get};
use serde_json::{Value, json};

#[tokio::test]
async fn cloud_output_and_endpoint_selection() {
    let app = json!({
        "app_slug":"demo", "status":"running", "hostname":"demo.example.com",
        "api_url":"https://demo.example.com:5005", "webui_url":"https://demo.example.com:8080"
    });
    let router = Router::new()
        .route(
            "/v1/apps/demo",
            get({
                let app = app.clone();
                move || {
                    let app = app.clone();
                    async move { Json(app) }
                }
            }),
        )
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
        .route("/v1/tokens", get(|| async { Json(json!({"tokens":[]})) }).post(|Json(body): Json<Value>| async move {
            assert!(body["hash"].as_str().unwrap().starts_with("sha256:"));
            assert!(body["global"] == true || !body["apps"].as_array().unwrap().is_empty());
            Json(json!({"token_id":"T_ID", "name":body["name"], "global":body["global"], "apps":body["apps"]}))
        }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let directory = tempfile::tempdir().unwrap();
    std::fs::write(directory.path().join("app.toml"), "invalid TOML [").unwrap();
    let run = async |args: &[&str]| {
        let output = tokio::process::Command::new(env!("CARGO_BIN_EXE_obelisk"))
            .args(["cloud", "--api-url", &origin])
            .args(args)
            .current_dir(directory.path())
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
        String::from_utf8(output.stdout).unwrap()
    };
    let endpoints = json!({"api_url":"https://demo.example.com:5005", "webhook_url":"https://demo.example.com", "webui_url":"https://demo.example.com:8080"});
    let text = run(&["app", "endpoint"]).await;
    for url in endpoints.as_object().unwrap().values() {
        assert!(text.contains(url.as_str().unwrap()), "{text}");
    }
    assert!(serde_json::from_str::<Value>(&text).is_err());
    assert_eq!(
        serde_json::from_str::<Value>(&run(&["app", "endpoint", "--json"]).await).unwrap(),
        endpoints
    );
    for (command, key) in [
        ("api", "api_url"),
        ("webhook", "webhook_url"),
        ("webui", "webui_url"),
    ] {
        assert_eq!(
            run(&["app", "endpoint", command]).await.trim(),
            endpoints[key].as_str().unwrap()
        );
        assert_eq!(
            serde_json::from_str::<Value>(&run(&["app", "endpoint", command, "--json"]).await)
                .unwrap(),
            endpoints[key]
        );
    }
    for command in [
        vec!["app", "get"],
        vec!["apps", "list"],
        vec!["token", "list"],
    ] {
        let text = run(&command).await;
        assert!(serde_json::from_str::<Value>(&text).is_err(), "{text}");
        let mut json_command = command.clone();
        json_command.push("--json");
        let parsed: Value = serde_json::from_str(&run(&json_command).await).unwrap();
        assert_eq!(
            parsed,
            match command[0] {
                "app" => app.clone(),
                "apps" => json!({"apps":[app]}),
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
    .await;
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
    .await;
    assert!(scoped.contains("scope: demo, worker"), "{scoped}");
    server.abort();
}
