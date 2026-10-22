use axum::{Json, Router, routing::get};
use serde_json::{Value, json};

#[tokio::test]
async fn cloud_app_endpoints_resolve_project_environment_and_explicit_targets_offline() {
    let directory = tempfile::tempdir().unwrap();
    let source = "# Project identity\napp_name='from-file'\n";
    std::fs::write(directory.path().join("app.toml"), source).unwrap();
    for (app_env, domain_env, arguments, expected_name, expected_domain) in [
        (
            None,
            None,
            vec!["endpoint", "--json"],
            "from-file",
            "obelisk.works",
        ),
        (
            Some("from-env"),
            Some("dev.obelisk.works"),
            vec!["--json", "endpoint"],
            "from-env",
            "dev.obelisk.works",
        ),
        (
            Some("from-env"),
            Some("ignored.example"),
            vec![
                "endpoint",
                "--app",
                "explicit",
                "--apps-domain",
                "test.obelisk.works",
                "--json",
            ],
            "explicit",
            "test.obelisk.works",
        ),
    ] {
        let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_obelisk"));
        command
            .args(["cloud", "--api-url", "http://127.0.0.1:1", "app"])
            .args(arguments)
            .current_dir(directory.path())
            .env_remove("OBELISK_APP_NAME")
            .env_remove("OBELISK_CLOUD_APPS_DOMAIN")
            .env_remove("OBELISK_CLOUD_API_TOKEN");
        if let Some(app_env) = app_env {
            command.env("OBELISK_APP_NAME", app_env);
        }
        if let Some(domain_env) = domain_env {
            command.env("OBELISK_CLOUD_APPS_DOMAIN", domain_env);
        }
        let output = command.output().await.unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            serde_json::from_slice::<Value>(&output.stdout).unwrap(),
            json!({
                "api_url":format!("https://api--{expected_name}.{expected_domain}"),
                "webhook_url":format!("https://{expected_name}.{expected_domain}"),
                "webui_url":format!("https://webui--{expected_name}.{expected_domain}"),
            })
        );
        assert_eq!(
            std::fs::read_to_string(directory.path().join("app.toml")).unwrap(),
            source
        );
    }
}

#[tokio::test]
async fn cloud_output_and_endpoint_selection() {
    let app = json!({
        "app_name":"demo", "status":"running", "hostname":"demo.example.com",
        "api_url":"https://api--demo.example.com", "webui_url":"https://webui--demo.example.com"
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
        .route(
            "/v1/apps/demo/status",
            get(|| async {
                Json(json!({
                    "app":{"app_name":"demo", "status":"stopped", "desired_state":"running"},
                    "machine_state":"started", "api_ready":false
                }))
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
            .args([
                "cloud",
                "--api-url",
                &origin,
                "--apps-domain",
                "example.com",
            ])
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
    let endpoints = json!({"api_url":"https://api--demo.example.com", "webhook_url":"https://demo.example.com", "webui_url":"https://webui--demo.example.com"});
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
    let status = run(&["app", "status"]).await;
    let apply = "Run `obelisk cloud app --app demo apply`";
    assert!(
        status.contains(&format!("Intended state: running. {apply} to reach it.")),
        "{status}"
    );
    assert!(
        status.contains(&format!(
            "VM is started although the app is stopped. {apply} to reconcile it."
        )),
        "{status}"
    );
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
    assert_eq!(
        std::fs::read_to_string(directory.path().join("ci.token")).unwrap(),
        "obk_secret"
    );
    server.abort();
}
