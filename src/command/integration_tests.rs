//! Integration tests for the Web API (REST + gRPC-Web).
//!
//! Each test starts a full Obelisk server with JS activity and workflow
//! components, exercises the REST API, and validates the results.
//!
//! The server is started in-process with `api.listening_addr = "127.0.0.1:0"`
//! and a oneshot channel to discover the actual bound port.  Each test gets
//! its own temporary TOML config and SQLite database directory.

use crate::{
    command::server::{RunParams, run_internal_with_ready_sender},
    config::config_holder::{ConfigFileOption, ConfigHolder, ConfigSource},
};
use directories::BaseDirs;
use serde_json::{json, Value};
use std::{path::PathBuf, time::Duration};
use tokio::sync::watch;

fn get_workspace_dir() -> PathBuf {
    PathBuf::from(std::env::var("CARGO_WORKSPACE_DIR").unwrap())
}

/// Write a minimal TOML config that uses port 0 (OS-assigned).
fn write_test_toml(toml_dir: &std::path::Path, db_dir: &std::path::Path) -> PathBuf {
    let workspace = get_workspace_dir();
    let add_js = workspace.join("crates/testing/test-programs/js/activity/add.js");
    let greet_js = workspace.join("crates/testing/test-programs/js/activity/greet.js");
    let add_wf_js = workspace.join("crates/testing/test-programs/js/workflow/add_workflow.js");

    let contents = format!(
        r#"
api.listening_addr = "127.0.0.1:0"

[database.sqlite]
directory = "{db_dir}"

[[activity_js]]
name = "test_add_activity"
location = "{add_js}"
ffqn = "testing:integration/activities.add"
params.inline = [
  {{ name = "a", type = "u32" }},
  {{ name = "b", type = "u32" }},
]
max_retries = 0

[[activity_js]]
name = "test_greet_activity"
location = "{greet_js}"
ffqn = "testing:integration/activities.greet"
params.inline = [
  {{ name = "name", type = "string" }},
]
max_retries = 0

[[workflow_js]]
name = "test_add_workflow"
location = "{add_wf_js}"
ffqn = "testing:integration/workflows.add-workflow"
params.inline = [
  {{ name = "a", type = "u32" }},
  {{ name = "b", type = "u32" }},
]
"#,
        db_dir = db_dir.display(),
        add_js = add_js.display(),
        greet_js = greet_js.display(),
        add_wf_js = add_wf_js.display(),
    );

    let toml_path = toml_dir.join("obelisk-integration-test.toml");
    std::fs::write(&toml_path, contents).expect("write toml");
    toml_path
}

struct TestServer {
    base_url: String,
    client: reqwest::Client,
    _termination_sender: watch::Sender<()>,
    _toml_dir: tempfile::TempDir,
    _db_dir: tempfile::TempDir,
}

impl TestServer {
    async fn start() -> Self {
        test_utils::set_up();

        let toml_dir = tempfile::tempdir().expect("create toml temp dir");
        let db_dir = tempfile::tempdir().expect("create db temp dir");
        let toml_path = write_test_toml(toml_dir.path(), db_dir.path());

        let project_dirs = crate::project_dirs();
        let base_dirs = BaseDirs::new();
        let config_holder = ConfigHolder::new(
            project_dirs,
            base_dirs,
            ConfigFileOption::MustExist(ConfigSource(toml_path)),
        )
        .unwrap();
        let config = config_holder.load_config().await.unwrap();

        let (termination_sender, termination_watcher) = watch::channel(());
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();

        let params = RunParams {
            clean_cache: false,
            clean_codegen_cache: false,
            clean_sqlite_directory: true,
            suppress_type_checking_errors: false,
        };

        tokio::spawn(async move {
            if let Err(e) = Box::pin(run_internal_with_ready_sender(
                config,
                config_holder,
                params,
                termination_watcher,
                Some(ready_tx),
            ))
            .await
            {
                eprintln!("Server error: {e:#}");
            }
        });

        let addr = tokio::time::timeout(Duration::from_secs(300), ready_rx)
            .await
            .expect("timed out waiting for server to bind")
            .expect("server task failed before binding");

        let base_url = format!("http://{addr}");

        TestServer {
            base_url,
            client: reqwest::Client::new(),
            _termination_sender: termination_sender,
            _toml_dir: toml_dir,
            _db_dir: db_dir,
        }
    }

    /// POST /v1/executions?follow=true
    async fn submit_follow(&self, ffqn: &str, params: Vec<Value>) -> reqwest::Response {
        self.client
            .post(format!("{}/v1/executions?follow=true", self.base_url))
            .header("Accept", "application/json")
            .json(&json!({ "ffqn": ffqn, "params": params }))
            .send()
            .await
            .expect("submit request failed")
    }

    /// PUT /v1/executions/{id}?follow=true
    async fn submit_follow_with_id(
        &self,
        execution_id: &str,
        ffqn: &str,
        params: Vec<Value>,
    ) -> reqwest::Response {
        self.client
            .put(format!(
                "{}/v1/executions/{execution_id}?follow=true",
                self.base_url
            ))
            .header("Accept", "application/json")
            .json(&json!({ "ffqn": ffqn, "params": params }))
            .send()
            .await
            .expect("submit request failed")
    }

    /// GET /v1/executions/{id}/events
    async fn get_events(&self, execution_id: &str) -> Value {
        self.client
            .get(format!(
                "{}/v1/executions/{execution_id}/events?length=100&direction=newer",
                self.base_url
            ))
            .header("Accept", "application/json")
            .send()
            .await
            .expect("events request failed")
            .json()
            .await
            .expect("events parse failed")
    }

    /// GET /v1/executions/{id}/logs
    async fn get_logs(&self, execution_id: &str) -> Value {
        self.client
            .get(format!(
                "{}/v1/executions/{execution_id}/logs?length=100&direction=newer",
                self.base_url
            ))
            .header("Accept", "application/json")
            .send()
            .await
            .expect("logs request failed")
            .json()
            .await
            .expect("logs parse failed")
    }

    /// GET /v1/executions/{id}/status
    async fn get_status(&self, execution_id: &str) -> Value {
        self.client
            .get(format!(
                "{}/v1/executions/{execution_id}/status",
                self.base_url
            ))
            .header("Accept", "application/json")
            .send()
            .await
            .expect("status request failed")
            .json()
            .await
            .expect("status parse failed")
    }

    /// PUT /v1/executions/{id}/replay
    async fn replay(&self, execution_id: &str) -> reqwest::Response {
        self.client
            .put(format!(
                "{}/v1/executions/{execution_id}/replay",
                self.base_url
            ))
            .header("Accept", "application/json")
            .send()
            .await
            .expect("replay request failed")
    }

    /// GET /v1/functions
    async fn list_functions(&self) -> Value {
        self.client
            .get(format!("{}/v1/functions", self.base_url))
            .header("Accept", "application/json")
            .send()
            .await
            .expect("functions request failed")
            .json()
            .await
            .expect("functions parse failed")
    }

    /// GET /v1/components
    async fn list_components(&self) -> Value {
        self.client
            .get(format!("{}/v1/components", self.base_url))
            .header("Accept", "application/json")
            .send()
            .await
            .expect("components request failed")
            .json()
            .await
            .expect("components parse failed")
    }

    /// GET /v1/executions
    async fn list_executions(&self) -> Value {
        self.client
            .get(format!("{}/v1/executions", self.base_url))
            .header("Accept", "application/json")
            .send()
            .await
            .expect("executions request failed")
            .json()
            .await
            .expect("executions parse failed")
    }
}

/// Sanitize dynamic fields in a JSON value for snapshot testing.
fn sanitize_json(value: &Value) -> Value {
    match value {
        Value::String(s) => {
            if s.starts_with("E_") && s.len() > 4 {
                Value::String("E_<REDACTED>".to_string())
            } else if s.starts_with("Dep_") && s.len() > 6 {
                Value::String("Dep_<REDACTED>".to_string())
            } else if s.starts_with("R_") && s.len() > 4 {
                Value::String("R_<REDACTED>".to_string())
            } else if s.starts_with("Run_") && s.len() > 6 {
                Value::String("Run_<REDACTED>".to_string())
            } else if s.starts_with("Exr_") && s.len() > 6 {
                Value::String("Exr_<REDACTED>".to_string())
            } else if s.starts_with("sha256:") {
                Value::String("sha256:<REDACTED>".to_string())
            } else if chrono::DateTime::parse_from_rfc3339(s).is_ok() {
                Value::String("<TIMESTAMP>".to_string())
            } else {
                value.clone()
            }
        }
        Value::Array(arr) => Value::Array(arr.iter().map(sanitize_json).collect()),
        Value::Object(map) => {
            let mut new_map = serde_json::Map::new();
            for (k, v) in map {
                new_map.insert(k.clone(), sanitize_json(v));
            }
            Value::Object(new_map)
        }
        _ => value.clone(),
    }
}

/// Sort a JSON array of objects by a string key for deterministic snapshots.
fn sort_json_array_by(value: &Value, key_path: &[&str]) -> Value {
    if let Value::Array(arr) = value {
        let mut sorted = arr.clone();
        sorted.sort_by(|a, b| {
            let a_key = key_path.iter().fold(a, |v, k| &v[k]);
            let b_key = key_path.iter().fold(b, |v, k| &v[k]);
            a_key
                .as_str()
                .unwrap_or("")
                .cmp(b_key.as_str().unwrap_or(""))
        });
        Value::Array(sorted)
    } else {
        value.clone()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn list_components_and_functions() {
    let server = TestServer::start().await;

    let components = server.list_components().await;
    let components = sort_json_array_by(&components, &["component_id", "name"]);
    let components = sanitize_json(&components);
    insta::assert_json_snapshot!("list_components", components);

    let functions = server.list_functions().await;
    let functions = sort_json_array_by(&functions, &["ffqn"]);
    let functions = sanitize_json(&functions);
    insta::assert_json_snapshot!("list_functions", functions);
}

#[tokio::test(flavor = "multi_thread")]
async fn submit_activity_and_get_result() {
    let server = TestServer::start().await;

    // add(3, 5) → "8" (activity_js returns result<string, string>)
    let resp = server
        .submit_follow(
            "testing:integration/activities.add",
            vec![json!(3), json!(5)],
        )
        .await;
    assert_eq!(resp.status().as_u16(), 201, "expected 201 Created");
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body, json!({"ok": "8"}));
}

#[tokio::test(flavor = "multi_thread")]
async fn submit_greet_activity_and_get_events() {
    let server = TestServer::start().await;

    let exec_id: String = server
        .client
        .get(format!("{}/v1/execution-id", server.base_url))
        .header("Accept", "application/json")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let resp = server
        .submit_follow_with_id(
            &exec_id,
            "testing:integration/activities.greet",
            vec![json!("World")],
        )
        .await;
    assert_eq!(resp.status().as_u16(), 201);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body, json!({"ok": "Hello, World!"}));

    let events = server.get_events(&exec_id).await;
    let events = sanitize_json(&events);
    insta::assert_json_snapshot!("greet_activity_events", events);

    let status = server.get_status(&exec_id).await;
    let status = sanitize_json(&status);
    insta::assert_json_snapshot!("greet_activity_status", status);
}

#[tokio::test(flavor = "multi_thread")]
async fn submit_greet_activity_and_get_logs() {
    let server = TestServer::start().await;

    let exec_id: String = server
        .client
        .get(format!("{}/v1/execution-id", server.base_url))
        .header("Accept", "application/json")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let resp = server
        .submit_follow_with_id(
            &exec_id,
            "testing:integration/activities.greet",
            vec![json!("World")],
        )
        .await;
    assert_eq!(resp.status().as_u16(), 201);

    // Allow log forwarding to flush.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let logs = server.get_logs(&exec_id).await;
    let logs = sanitize_json(&logs);
    insta::assert_json_snapshot!("greet_activity_logs", logs);
}

#[tokio::test(flavor = "multi_thread")]
async fn submit_workflow_and_get_result() {
    let server = TestServer::start().await;

    let resp = server
        .submit_follow(
            "testing:integration/workflows.add-workflow",
            vec![json!(10), json!(20)],
        )
        .await;
    assert_eq!(resp.status().as_u16(), 201);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body, json!({"ok": "30"}));
}

#[tokio::test(flavor = "multi_thread")]
async fn submit_workflow_and_replay() {
    let server = TestServer::start().await;

    let exec_id: String = server
        .client
        .get(format!("{}/v1/execution-id", server.base_url))
        .header("Accept", "application/json")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let resp = server
        .submit_follow_with_id(
            &exec_id,
            "testing:integration/workflows.add-workflow",
            vec![json!(10), json!(20)],
        )
        .await;
    assert_eq!(resp.status().as_u16(), 201);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body, json!({"ok": "30"}));

    let events_before = server.get_events(&exec_id).await;
    let events_before = sanitize_json(&events_before);
    insta::assert_json_snapshot!("workflow_add_events", events_before);

    let replay_resp = server.replay(&exec_id).await;
    assert_eq!(
        replay_resp.status().as_u16(),
        200,
        "replay failed: {}",
        replay_resp.text().await.unwrap()
    );

    let events_after = server.get_events(&exec_id).await;
    let events_after = sanitize_json(&events_after);
    assert_eq!(events_before, events_after);
}

#[tokio::test(flavor = "multi_thread")]
async fn list_executions_after_submit() {
    let server = TestServer::start().await;

    let resp = server
        .submit_follow(
            "testing:integration/activities.add",
            vec![json!(1), json!(2)],
        )
        .await;
    assert_eq!(resp.status().as_u16(), 201);

    let executions = server.list_executions().await;
    let arr = executions.as_array().expect("expected array");
    assert_eq!(arr.len(), 1, "expected 1 execution, got: {executions:?}");
    assert_eq!(arr[0]["ffqn"], json!("testing:integration/activities.add"));
}

#[tokio::test(flavor = "multi_thread")]
async fn submit_wrong_params_returns_error() {
    let server = TestServer::start().await;

    let resp = server
        .client
        .post(format!("{}/v1/executions", server.base_url))
        .header("Accept", "application/json")
        .json(&json!({
            "ffqn": "testing:integration/activities.add",
            "params": [1]
        }))
        .send()
        .await
        .unwrap();

    assert!(
        resp.status().is_client_error(),
        "expected 4xx for wrong params, got {}",
        resp.status()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn submit_nonexistent_function_returns_404() {
    let server = TestServer::start().await;

    let resp = server
        .client
        .post(format!("{}/v1/executions", server.base_url))
        .header("Accept", "application/json")
        .json(&json!({
            "ffqn": "testing:nonexistent/ifc.fn",
            "params": []
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status().as_u16(), 404);
}

#[tokio::test(flavor = "multi_thread")]
async fn replay_nonexistent_execution_returns_404() {
    let server = TestServer::start().await;

    let resp = server.replay("E_01AAAAAAAAAAAAAAAAAAAAAAAA").await;
    assert_eq!(resp.status().as_u16(), 404);
}

#[tokio::test(flavor = "multi_thread")]
async fn idempotent_submit_same_execution_id() {
    let server = TestServer::start().await;

    let exec_id: String = server
        .client
        .get(format!("{}/v1/execution-id", server.base_url))
        .header("Accept", "application/json")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let resp1 = server
        .submit_follow_with_id(
            &exec_id,
            "testing:integration/activities.add",
            vec![json!(1), json!(2)],
        )
        .await;
    assert_eq!(resp1.status().as_u16(), 201);
    let body1: Value = resp1.json().await.unwrap();

    let resp2 = server
        .submit_follow_with_id(
            &exec_id,
            "testing:integration/activities.add",
            vec![json!(1), json!(2)],
        )
        .await;
    assert_eq!(
        resp2.status().as_u16(),
        200,
        "second submit should return 200"
    );
    let body2: Value = resp2.json().await.unwrap();
    assert_eq!(body1, body2, "both submits should return same result");
}
