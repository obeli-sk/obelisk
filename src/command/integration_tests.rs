//! Integration tests for the Web API.
//!
//! Each test spins up a full Obelisk server (compile → DB → HTTP) against a
//! temporary TOML config that references the JS test-program fixtures.
//! The server binds to a free port discovered at test start, so tests can
//! run in parallel without conflicts.

use crate::{
    command::server::{RunParams, run_internal},
    config::config_holder::{ConfigFileOption, ConfigHolder, ConfigSource},
};
use directories::BaseDirs;
use serde_json::{Value, json};
use std::{path::PathBuf, time::Duration};
use tokio::sync::watch;

#[cfg(test)]
mod populate_codegen_cache {

    #[tokio::test]
    async fn test_server() {
        super::TestServer::start().await;
    }
}

fn get_workspace_dir() -> PathBuf {
    PathBuf::from(std::env::var("CARGO_WORKSPACE_DIR").unwrap())
}

/// Bind to port 0, capture the assigned port, then drop the listener.
fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

/// Write a minimal TOML config to a temp file and return the path.
/// The config references the JS fixtures from the workspace tree and
/// places the `SQLite` database in a unique temp directory.
fn write_test_toml(port: u16) -> (tempfile::TempDir, PathBuf) {
    let workspace = get_workspace_dir();
    let db_dir = tempfile::tempdir().unwrap();
    let toml_contents = format!(
        r#"
api.listening_addr = "127.0.0.1:{port}"

[wasm.codegen_cache]
directory = "${{CACHE_DIR}}/codegen-it"

[database.sqlite]
directory = "{db_dir}"

[[activity_js]]
name = "test_add_activity"
location = "{ws}/crates/testing/test-programs/js/activity/add.js"
ffqn = "testing:integration/activities.add"
params.inline = [
  {{ name = "a", type = "u32" }},
  {{ name = "b", type = "u32" }},
]
max_retries = 0

[[activity_js]]
name = "test_greet_activity"
location = "{ws}/crates/testing/test-programs/js/activity/greet.js"
ffqn = "testing:integration/activities.greet"
params.inline = [
  {{ name = "name", type = "string" }},
]
max_retries = 0

[[workflow_js]]
name = "test_add_workflow"
location = "{ws}/crates/testing/test-programs/js/workflow/add_workflow.js"
ffqn = "testing:integration/workflows.add-workflow"
params.inline = [
  {{ name = "a", type = "u32" }},
  {{ name = "b", type = "u32" }},
]

[[workflow_js]]
name = "test_add_via_activity_workflow"
location = "{ws}/crates/testing/test-programs/js/workflow/add_via_activity.js"
ffqn = "testing:integration/workflows.add-via-activity"
params.inline = [
  {{ name = "a", type = "u32" }},
  {{ name = "b", type = "u32" }},
]
"#,
        port = port,
        db_dir = db_dir.path().display(),
        ws = workspace.display(),
    );
    let toml_path = db_dir.path().join("obelisk-test.toml");
    std::fs::write(&toml_path, toml_contents).unwrap();
    (db_dir, toml_path)
}

struct TestServer {
    base_url: String,
    client: reqwest::Client,
    _termination_sender: watch::Sender<()>,
    _tmp_dir: tempfile::TempDir,
}

impl TestServer {
    async fn start() -> Self {
        test_utils::set_up();

        let port = free_port();
        let (tmp_dir, toml_path) = write_test_toml(port);

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

        let params = RunParams {
            clean_cache: false,
            clean_codegen_cache: false,
            clean_sqlite_directory: true,
            suppress_type_checking_errors: false,
        };

        tokio::spawn(async move {
            if let Err(e) = Box::pin(run_internal(
                config,
                config_holder,
                params,
                termination_watcher,
            ))
            .await
            {
                eprintln!("Server error: {e:#}");
            }
        });

        let base_url = format!("http://127.0.0.1:{port}");
        let client = reqwest::Client::new();

        // Poll until the server is ready.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
        loop {
            match client
                .get(format!("{base_url}/v1/functions"))
                .header("Accept", "application/json")
                .send()
                .await
            {
                Ok(resp) if resp.status().is_success() => break,
                _ if tokio::time::Instant::now() > deadline => {
                    panic!("server did not become ready within 120 s")
                }
                _ => tokio::time::sleep(Duration::from_millis(200)).await,
            }
        }

        TestServer {
            base_url,
            client,
            _termination_sender: termination_sender,
            _tmp_dir: tmp_dir,
        }
    }

    // ---- helper methods ------------------------------------------------

    async fn submit_follow(&self, ffqn: &str, params: Vec<Value>) -> reqwest::Response {
        self.client
            .post(format!("{}/v1/executions?follow=true", self.base_url))
            .header("Accept", "application/json")
            .json(&json!({ "ffqn": ffqn, "params": params }))
            .send()
            .await
            .expect("submit request failed")
    }

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

    async fn generate_execution_id(&self) -> String {
        self.client
            .get(format!("{}/v1/execution-id", self.base_url))
            .header("Accept", "application/json")
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap()
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

// ---- Component / function listing ----

#[tokio::test]
async fn list_components() {
    let server = TestServer::start().await;

    let components = server.list_components().await;
    let components = sanitize_json(&components);
    insta::assert_json_snapshot!("list_components", components);

    let functions = server.list_functions().await;
    let functions = sanitize_json(&functions);
    insta::assert_json_snapshot!("list_functions", functions);
}

#[tokio::test]
async fn list_functions() {
    let server = TestServer::start().await;

    let functions = server.list_functions().await;
    let functions = sanitize_json(&functions);
    insta::assert_json_snapshot!("list_functions", functions);
}

// ---- Activity: submit + result ----

#[tokio::test]
async fn submit_activity_and_get_result() {
    let server = TestServer::start().await;

    let resp = server
        .submit_follow(
            "testing:integration/activities.add",
            vec![json!(3), json!(5)],
        )
        .await;
    assert_eq!(resp.status().as_u16(), 201);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body, json!({ "ok": "8" }));
}

// ---- Activity: submit + events / logs / status snapshots ----

#[tokio::test]
async fn submit_greet_activity_and_inspect() {
    let server = TestServer::start().await;
    let exec_id = server.generate_execution_id().await;

    let resp = server
        .submit_follow_with_id(
            &exec_id,
            "testing:integration/activities.greet",
            vec![json!("World")],
        )
        .await;
    assert_eq!(resp.status().as_u16(), 201);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body, json!({ "ok": "Hello, World!" }));

    let events = server.get_events(&exec_id).await;
    let events = sanitize_json(&events);
    insta::assert_json_snapshot!("greet_activity_events", events);

    // Allow log forwarding to flush.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let logs = server.get_logs(&exec_id).await;
    let logs = sanitize_json(&logs);
    insta::assert_json_snapshot!("greet_activity_logs", logs);

    let status = server.get_status(&exec_id).await;
    let status = sanitize_json(&status);
    insta::assert_json_snapshot!("greet_activity_status", status);
}

// ---- Workflow: submit + events + replay ----

#[tokio::test]
async fn submit_workflow_and_replay() {
    let server = TestServer::start().await;
    let exec_id = server.generate_execution_id().await;

    // JS workflows return result<string, string>.
    let resp = server
        .submit_follow_with_id(
            &exec_id,
            "testing:integration/workflows.add-workflow",
            vec![json!(10), json!(20)],
        )
        .await;
    assert_eq!(resp.status().as_u16(), 201);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body, json!({ "ok": "30" }));

    let events = server.get_events(&exec_id).await;
    let events = sanitize_json(&events);
    insta::assert_json_snapshot!("workflow_add_events", events);

    let replay_resp = server.replay(&exec_id).await;
    assert_eq!(
        replay_resp.status().as_u16(),
        200,
        "replay failed: {}",
        replay_resp.text().await.unwrap()
    );
    let events_after = server.get_events(&exec_id).await;
    let events_after = sanitize_json(&events_after);
    assert_eq!(
        events, events_after,
        "events must be identical after replay"
    );
}

// ---- Workflow: submit activity via join set + getResult ----

#[tokio::test]
async fn submit_workflow_with_get_result() {
    let server = TestServer::start().await;
    let exec_id = server.generate_execution_id().await;

    let resp = server
        .submit_follow_with_id(
            &exec_id,
            "testing:integration/workflows.add-via-activity",
            vec![json!(7), json!(8)],
        )
        .await;
    assert_eq!(resp.status().as_u16(), 201);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body, json!({ "ok": "15" }));

    let events = server.get_events(&exec_id).await;
    let events = sanitize_json(&events);
    insta::assert_json_snapshot!("workflow_add_via_activity_events", events);
}

// ---- Execution listing ----

#[tokio::test]
async fn list_executions_after_submit() {
    let server = TestServer::start().await;

    let resp = server
        .submit_follow(
            "testing:integration/activities.add",
            vec![json!(1), json!(2)],
        )
        .await;
    assert_eq!(resp.status().as_u16(), 201);
    let _: Value = resp.json().await.unwrap();

    let executions = server.list_executions().await;
    let arr = executions.as_array().expect("array");
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["ffqn"], json!("testing:integration/activities.add"));
}

// ---- Error cases ----

#[tokio::test]
async fn submit_with_wrong_params_returns_error() {
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
    assert_eq!(resp.status().as_u16(), 400);
}

#[tokio::test]
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

#[tokio::test]
async fn replay_nonexistent_execution_returns_404() {
    let server = TestServer::start().await;
    let resp = server.replay("E_01AAAAAAAAAAAAAAAAAAAAAAAA").await;
    assert_eq!(resp.status().as_u16(), 404);
}

// ---- Idempotency ----

#[tokio::test]
async fn idempotent_submit_same_execution_id() {
    let server = TestServer::start().await;
    let exec_id = server.generate_execution_id().await;

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
    assert_eq!(resp2.status().as_u16(), 200);
    let body2: Value = resp2.json().await.unwrap();
    assert_eq!(body1, body2);
}
