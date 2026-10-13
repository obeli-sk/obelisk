use std::process::{Command, Output};

fn cli(dir: &std::path::Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_obelisk"))
        .args(args)
        .current_dir(dir)
        .env_remove("OBELISK_APP_NAME")
        .env_remove("OBELISK_UNSTABLE_ACTIVITY_VM")
        .env("OBELISK_JS_RUNTIME", "v8")
        .output()
        .unwrap()
}

fn assert_success(output: &Output) {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn server_app_discovery_preserves_explicit_paths_and_rejects_invalid_files() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("deployment.toml"), "").unwrap();
    std::fs::write(dir.path().join("app.toml"), "app_name='discovered'\n").unwrap();
    let args = ["server", "verify", "--skip-db", "-d", "deployment.toml"];
    assert_success(&cli(dir.path(), &args));

    std::fs::write(dir.path().join("app.toml"), "invalid TOML [").unwrap();
    for command in ["run", "verify"] {
        let output = cli(dir.path(), &["server", command]);
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr).contains("cannot parse app config"));
    }
    std::fs::write(dir.path().join("explicit.toml"), "app_name='explicit'\n").unwrap();
    let mut explicit = args.to_vec();
    explicit.extend(["-a", "explicit.toml"]);
    assert_success(&cli(dir.path(), &explicit));

    std::fs::remove_file(dir.path().join("app.toml")).unwrap();
    let output = cli(dir.path(), &args);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("app name is required"));
    let output = Command::new(env!("CARGO_BIN_EXE_obelisk"))
        .args(args)
        .current_dir(dir.path())
        .env("OBELISK_APP_NAME", "from-env")
        .env("OBELISK_JS_RUNTIME", "v8")
        .env_remove("OBELISK_UNSTABLE_ACTIVITY_VM")
        .output()
        .unwrap();
    assert_success(&output);

    let mut repairs = args.to_vec();
    repairs.push("--fix");
    let output = cli(dir.path(), &repairs);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("app repairs require"));
    assert!(!dir.path().join("app.toml").exists());
}

#[tokio::test]
async fn cloud_new_and_update_load_local_policy() {
    use axum::{Json, body::Bytes, routing::any};
    use serde_json::{Value, json};
    use std::sync::{Arc, Mutex};

    let requests = Arc::new(Mutex::new(Vec::new()));
    let received = requests.clone();
    let router = axum::Router::new().fallback(any(
        move |method: axum::http::Method, uri: axum::http::Uri, body: Bytes| {
            let received = received.clone();
            async move {
                received.lock().unwrap().push((
                    method.to_string(),
                    uri.to_string(),
                    serde_json::from_slice::<Value>(&body).unwrap(),
                ));
                Json(json!({"ok":true}))
            }
        },
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("app.toml"),
        "app_name='local-app'\n[public_env]\nREGION={}\n",
    )
    .unwrap();
    tokio::task::spawn_blocking(move || {
        for command in ["new", "update"] {
            assert_success(&cli(
                dir.path(),
                &[
                    "cloud",
                    "--api-url",
                    &origin,
                    "--api-token",
                    "test-token",
                    "app",
                    command,
                ],
            ));
        }
    })
    .await
    .unwrap();
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].0, "POST");
    assert_eq!(requests[0].1, "/v1/apps");
    assert_eq!(requests[0].2["app_slug"], "local-app");
    assert_eq!(requests[0].2["policy"]["public_env"], json!(["REGION"]));
    assert_eq!(requests[1].0, "PUT");
    assert_eq!(requests[1].1, "/v1/apps/local-app/policy");
    assert_eq!(requests[1].2, requests[0].2["policy"]);
    task.abort();
}
