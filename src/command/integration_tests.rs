//! Integration tests for the Web API.
//!
//! Each test spins up a full Obelisk server (compile → DB → HTTP) against a
//! temporary TOML config that references the JS test-program fixtures.
//!
//! Each test uses a unique loopback address in the 127.1.0.0/16 range with
//! fixed ports, allowing parallel test execution without conflicts.
//! The `test_addr!` macro ensures unique addresses at link time.

use crate::config::toml::{
    ActivityStubComponentConfigToml, ActivityStubInlineConfigToml, ConfigName,
};
use crate::{
    command::server::{PrepareDirsParams, RunParams, prepare_dirs, run_internal},
    config::{
        config_holder::{ConfigHolder, load_deployment_toml},
        env_var::EnvVarConfig,
        toml::DeploymentCanonical,
    },
};
use chrono::Utc;
use concepts::prefixed_ulid::DeploymentId;
use concepts::storage::{DbPool as _, DbPoolCloseable as _, DeploymentRecord, DeploymentStatus};
use db_sqlite::sqlite_dao::{SqliteConfig, SqlitePool};
use directories::BaseDirs;
use grpc::grpc_gen::{
    DeploymentId as GrpcDeploymentId, ListComponentsRequest, SwitchDeploymentRequest,
    deployment_repository_client::DeploymentRepositoryClient,
    function_repository_client::FunctionRepositoryClient, switch_deployment_response::Outcome,
};
use hmac::{Hmac, Mac};
use serde_json::{Value, json};
use sha2::Sha256;
use std::fmt::Write as _;
use std::sync::Arc;
use std::{path::PathBuf, time::Duration};
use tokio::{sync::watch, task::JoinHandle};
use tracing::debug;

#[cfg(test)]
mod populate_js_codegen_cache {
    use super::test_addr;

    #[tokio::test]
    async fn test_server() {
        super::TestServer::start(test_addr!(1))
            .await
            .shutdown()
            .await;
    }
}

fn get_workspace_dir() -> PathBuf {
    PathBuf::from(std::env::var("CARGO_WORKSPACE_DIR").unwrap())
}

/// Fixed ports used by all integration tests.
/// Each test uses a unique IP address in 127.1.0.0/16, so ports don't conflict.
const API_PORT: u16 = 9080;
const WEBHOOK_PORT: u16 = 9081;

/// Generate a unique loopback address for a test.
///
/// Uses `127.1.{id/256}.{id%256}` to derive the address from the ID.
/// The macro also generates a static symbol that will cause a linker error
/// if two tests use the same ID.
macro_rules! test_addr {
    ($id:literal) => {{
        paste::paste! {
            #[used]
            #[unsafe(no_mangle)]
            #[allow(non_upper_case_globals)]
            static [<__obelisk_it_addr_ $id>]: () = ();
        }
        format!("127.1.{}.{}", ($id as u16) / 256, ($id as u16) % 256)
    }};
}
pub(crate) use test_addr;

/// Write separate server and deployment TOML configs to temp files and return their paths.
/// The server config includes API, DB, and wasm settings.
/// The deployment config references the JS fixtures from the workspace tree.
fn write_test_configs(ip: &str) -> (tempfile::TempDir, PathBuf, PathBuf) {
    let workspace = get_workspace_dir();
    let db_dir = tempfile::tempdir().unwrap();
    let server_contents = format!(
        r#"api.listening_addr = "{ip}:{API_PORT}"
webui.enabled = false
external.listening_addr = "{ip}:{WEBHOOK_PORT}"

[wasm.codegen_cache]
directory = "${{CACHE_DIR}}/codegen-it"

[database.sqlite]
directory = "{db_dir}"
"#,
        ip = ip,
        API_PORT = API_PORT,
        WEBHOOK_PORT = WEBHOOK_PORT,
        db_dir = db_dir.path().display(),
    );
    let server_path = db_dir.path().join("obelisk-test-server.toml");
    std::fs::write(&server_path, server_contents).unwrap();

    let ws = workspace.display();
    let deployment_contents = format!(
        r#"
[[activity_js]]
name = "test_add_activity"
location = "{ws}/crates/testing/test-programs/js/activity/add.js"
ffqn = "testing:integration/activity-add.add"
params = [
  {{ name = "a", type = "u32" }},
  {{ name = "b", type = "u32" }},
]
return_type = "result<string, string>"
max_retries = 0

[[activity_js]]
name = "test_greet_activity"
location = "{ws}/crates/testing/test-programs/js/activity/greet.js"
ffqn = "testing:integration/activity-greet.greet"
params = [
  {{ name = "name", type = "string" }},
]
return_type = "result<string, string>"
max_retries = 0

[[activity_js]]
name = "test_fetch_denied_activity"
location = "{ws}/crates/testing/test-programs/js/activity/fetch_get.js"
ffqn = "testing:integration/fetch-get-denied.fetch-get"
params = [
  {{ name = "url", type = "string" }},
  {{ name = "headers", type = "list<tuple<string,string>>" }},
]
return_type = "result<string, string>"
max_retries = 0

[[activity_js]]
name = "test_fetch_allowed_activity"
location = "{ws}/crates/testing/test-programs/js/activity/fetch_get.js"
ffqn = "testing:integration/fetch-get-allowed.fetch-get"
params = [
  {{ name = "url", type = "string" }},
  {{ name = "headers", type = "list<tuple<string,string>>" }},
]
return_type = "result<string, string>"
max_retries = 0
[[activity_js.allowed_host]]
pattern = "http://{ip}:{API_PORT}"
methods = ["GET"]

[[activity_js]]
name = "test_read_env_activity"
location = "{ws}/crates/testing/test-programs/js/activity/read_env.js"
ffqn = "testing:integration/activity-env.read-env"
params = [
  {{ name = "key", type = "string" }},
]
return_type = "result<string, string>"
max_retries = 0
env_vars = [{{key = "TEST_ENV_VAR", value = "hello_from_env"}}]

[[activity_js]]
name = "test_make_record_activity"
location = "{ws}/crates/testing/test-programs/js/activity/make_record.js"
ffqn = "testing:integration/activity-make-record.make-record"
params = [
  {{ name = "name", type = "string" }},
]
return_type = "result<record {{ name: string, count: u32 }}, string>"
max_retries = 0

[[activity_js]]
name = "test_throw_variant_activity"
location = "{ws}/crates/testing/test-programs/js/activity/throw_variant.js"
ffqn = "testing:integration/activity-throw-variant.throw-variant"
params = []
return_type = "result<u32, variant {{ execution-failed, not-found }}>"
max_retries = 0

[[activity_js]]
name = "test_throw_null_activity"
location = "{ws}/crates/testing/test-programs/js/activity/throw_null.js"
ffqn = "testing:integration/activity-throw-null.throw-null"
params = []
return_type = "result<string>"
max_retries = 0

[[workflow_js]]
name = "test_add_workflow"
location = "{ws}/crates/testing/test-programs/js/workflow/add_workflow.js"
ffqn = "testing:integration/workflow-add.add-workflow"
params = [
  {{ name = "a", type = "u32" }},
  {{ name = "b", type = "u32" }},
]
return_type = "result<string, string>"

[[workflow_js]]
name = "test_add_via_activity_workflow"
location = "{ws}/crates/testing/test-programs/js/workflow/add_via_activity.js"
ffqn = "testing:integration/workflow-add-via-activity.add-via-activity"
params = [
  {{ name = "a", type = "u32" }},
  {{ name = "b", type = "u32" }},
]
return_type = "result<string, string>"

[[workflow_js]]
name = "test_call_activity_workflow"
location = "{ws}/crates/testing/test-programs/js/workflow/call_activity.js"
ffqn = "testing:integration/workflow-call-activity.call-activity"
params = [
  {{ name = "a", type = "u32" }},
  {{ name = "b", type = "u32" }},
]
return_type = "result<string, string>"

[[workflow_js]]
name = "test_make_record_workflow"
location = "{ws}/crates/testing/test-programs/js/workflow/make_record.js"
ffqn = "testing:integration/workflow-make-record.make-record"
params = [
  {{ name = "name", type = "string" }},
]
return_type = "result<record {{ name: string, count: u32 }}, string>"

[[workflow_js]]
name = "test_throw_variant_workflow"
location = "{ws}/crates/testing/test-programs/js/workflow/throw_variant.js"
ffqn = "testing:integration/workflow-throw-variant.throw-variant"
params = []
return_type = "result<u32, variant {{ execution-failed, not-found }}>"

[[workflow_js]]
name = "test_throw_null_workflow"
location = "{ws}/crates/testing/test-programs/js/workflow/throw_null.js"
ffqn = "testing:integration/workflow-throw-null.throw-null"
params = []
return_type = "result<string>"

[[workflow_js]]
name = "test_call_stub_workflow"
location = "{ws}/crates/testing/test-programs/js/workflow/call_stub.js"
ffqn = "testing:integration/workflow-call-stub.call-stub"
params = [
  {{ name = "id", type = "u64" }},
]
return_type = "result<string, string>"

[[workflow_js]]
name = "test_math_random_workflow"
location = "{ws}/crates/testing/test-programs/js/workflow/math_random.js"
ffqn = "testing:integration/workflow-math-random.math-random"
params = []
return_type = "result<string, string>"

[[workflow_js]]
name = "test_date_now_workflow"
location = "{ws}/crates/testing/test-programs/js/workflow/date_now.js"
ffqn = "testing:integration/workflow-date-now.date-now"
params = []
return_type = "result<string, string>"

[[activity_js]]
name = "test_hmac_sign_verify_activity"
location = "{ws}/crates/testing/test-programs/js/activity/hmac_sign_verify.js"
ffqn = "testing:integration/activity-hmac.hmac-sign-verify"
params = [
  {{ name = "key", type = "string" }},
  {{ name = "message", type = "string" }},
]
return_type = "result<string, string>"
max_retries = 0

[[activity_stub]]
name = "test_inline_stub"
ffqn = "testing:integration/stubs.my-stub"
params = [
  {{ name = "id", type = "u64" }},
]
return_type = "result<string, string>"


[[webhook_endpoint_js]]
name = "test_hello_webhook"
location = "{ws}/crates/testing/test-programs/js/webhook/hello.js"
routes = [{{ methods = ["GET"], route = "/hello" }}]

[[webhook_endpoint_js]]
name = "test_headers_webhook"
location = "{ws}/crates/testing/test-programs/js/webhook/headers.js"
routes = [{{ methods = ["GET"], route = "/headers" }}]

[[webhook_endpoint_js]]
name = "test_fetch_allowed_webhook"
location = "{ws}/crates/testing/test-programs/js/webhook/fetch_components.js"
routes = [{{ methods = ["GET"], route = "/fetch-allowed" }}]
[[webhook_endpoint_js.allowed_host]]
pattern = "http://{ip}:{API_PORT}"
methods = ["GET"]

[[webhook_endpoint_js]]
name = "test_fetch_denied_webhook"
location = "{ws}/crates/testing/test-programs/js/webhook/fetch_components.js"
routes = [{{ methods = ["GET"], route = "/fetch-denied" }}]

[[webhook_endpoint_js]]
name = "test_call_activity_webhook"
location = "{ws}/crates/testing/test-programs/js/webhook/call_activity.js"
routes = [{{ methods = ["GET"], route = "/call-activity/:a/:b" }}]

[[webhook_endpoint_js]]
name = "test_read_env_webhook"
location = "{ws}/crates/testing/test-programs/js/webhook/read_env.js"
routes = [{{ methods = ["GET"], route = "/read-env" }}]
env_vars = [{{key = "WEBHOOK_TEST_ENV_VAR", value = "hello_from_webhook_env"}}]
"#,
    );
    debug!("Deployment TOML:{deployment_contents}");
    let deployment_path = db_dir.path().join("obelisk-test-deployment.toml");
    std::fs::write(&deployment_path, deployment_contents).unwrap();
    (db_dir, server_path, deployment_path)
}

struct TestServer {
    ip: String,
    base_url: String,
    webhook_base_url: String,
    client: reqwest::Client,
    termination_sender: watch::Sender<()>,
    server_handle: JoinHandle<anyhow::Result<()>>,
    sqlite_file: std::path::PathBuf,
    _tmp_dir: tempfile::TempDir,
}

impl TestServer {
    async fn start(ip: String) -> Self {
        test_utils::set_up();

        let (tmp_dir, server_path, deployment_path) = write_test_configs(&ip);

        let project_dirs = crate::project_dirs();
        let base_dirs = BaseDirs::new();
        let config_holder = ConfigHolder::new(project_dirs, base_dirs, Some(server_path)).unwrap();
        let config = config_holder.load_config().await.unwrap();

        let (deployment_toml, deployment_dir) =
            load_deployment_toml(deployment_path).await.unwrap();
        let mut path_prefixes = config_holder.path_prefixes;
        path_prefixes.deployment_dir = Some(deployment_dir);

        let (termination_sender, termination_watcher) = watch::channel(());

        let params = RunParams {
            dir_params: PrepareDirsParams::default(),
            clean_sqlite_directory: false,
            suppress_type_checking_errors: false,
        };

        let prepared_dirs = prepare_dirs(&config, &params.dir_params, &path_prefixes)
            .await
            .unwrap();

        let server_handle = tokio::spawn(async move {
            Box::pin(run_internal(
                config,
                Some(deployment_toml),
                Arc::new(path_prefixes),
                params,
                prepared_dirs,
                termination_watcher,
            ))
            .await
        });

        let base_url = format!("http://{ip}:{API_PORT}");
        let client = reqwest::Client::new();

        // Poll until the server is ready.
        loop {
            if server_handle.is_finished() {
                server_handle.await.unwrap().unwrap();
                unreachable!("server must have panicked")
            }
            debug!("Pinging sever");
            let resp = client
                .get(format!("{base_url}/v1/functions"))
                .header("Accept", "application/json")
                .send()
                .await;
            if let Ok(resp) = resp
                && resp.status().is_success()
            {
                break;
            }
            debug!("Pinging sever failed");
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        let webhook_base_url = format!("http://{ip}:{WEBHOOK_PORT}");
        let sqlite_file = tmp_dir.path().join(crate::config::toml::SQLITE_FILE_NAME);
        TestServer {
            ip,
            base_url,
            webhook_base_url,
            client,
            termination_sender,
            server_handle,
            sqlite_file,
            _tmp_dir: tmp_dir,
        }
    }

    /// Gracefully shut down the server and wait for it to finish.
    async fn shutdown(self) {
        let Self {
            server_handle,
            termination_sender,
            ..
        } = self;
        drop(termination_sender); // signals shutdown
        let _ = server_handle.await;
    }

    // ---- helper methods ------------------------------------------------

    fn api_addr(&self) -> String {
        format!("{}:{}", self.ip, API_PORT)
    }

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

    async fn get_backtrace_source(
        &self,
        execution_id: &str,
        file: &str,
        filter: Option<&str>,
    ) -> reqwest::Response {
        let filter_part = match filter {
            Some(f) => format!("&filter={f}"),
            None => String::new(),
        };
        self.client
            .get(format!(
                "{}/v1/executions/{execution_id}/backtrace/source?file={file}{filter_part}",
                self.base_url
            ))
            .header("Accept", "application/json")
            .send()
            .await
            .expect("backtrace/source request failed")
    }

    async fn get_backtrace(&self, execution_id: &str, filter: Option<&str>) -> reqwest::Response {
        let url = match filter {
            Some(f) => format!(
                "{}/v1/executions/{execution_id}/backtrace?filter={f}",
                self.base_url
            ),
            None => format!("{}/v1/executions/{execution_id}/backtrace", self.base_url),
        };
        self.client
            .get(url)
            .header("Accept", "application/json")
            .send()
            .await
            .expect("backtrace request failed")
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
    let server = TestServer::start(test_addr!(2)).await;

    let components = server.list_components().await;
    let components = sanitize_json(&components);
    insta::assert_json_snapshot!("list_components", components);
    server.shutdown().await;
}

#[tokio::test]
async fn list_functions() {
    let server = TestServer::start(test_addr!(3)).await;

    let functions = server.list_functions().await;
    let functions = sanitize_json(&functions);
    insta::assert_json_snapshot!("list_functions", functions);
    server.shutdown().await;
}

// ---- Activity: submit + result ----

#[tokio::test]
async fn submit_activity_and_get_result() {
    let server = TestServer::start(test_addr!(4)).await;

    let resp = server
        .submit_follow(
            "testing:integration/activity-add.add",
            vec![json!(3), json!(5)],
        )
        .await;
    assert_eq!(resp.status().as_u16(), 201);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body, json!({ "ok": "8" }));
    server.shutdown().await;
}

// ---- Activity: submit + events snapshot ----

#[tokio::test]
async fn greet_activity_events() {
    let server = TestServer::start(test_addr!(5)).await;
    let exec_id = server.generate_execution_id().await;

    let resp = server
        .submit_follow_with_id(
            &exec_id,
            "testing:integration/activity-greet.greet",
            vec![json!("World")],
        )
        .await;
    assert_eq!(resp.status().as_u16(), 201);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body, json!({ "ok": "Hello, World!" }));

    let events = server.get_events(&exec_id).await;
    let events = sanitize_json(&events);
    insta::assert_json_snapshot!("greet_activity_events", events);
    server.shutdown().await;
}

// ---- Activity: submit + logs snapshot ----

#[tokio::test]
async fn greet_activity_logs() {
    let server = TestServer::start(test_addr!(6)).await;
    let exec_id = server.generate_execution_id().await;

    let resp = server
        .submit_follow_with_id(
            &exec_id,
            "testing:integration/activity-greet.greet",
            vec![json!("World")],
        )
        .await;
    assert_eq!(resp.status().as_u16(), 201);
    // Consume the streamed body to wait for execution to finish.
    let _: Value = resp.json().await.unwrap();

    // Allow log forwarding to flush.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let logs = server.get_logs(&exec_id).await;
    let logs = sanitize_json(&logs);
    insta::assert_json_snapshot!("greet_activity_logs", logs);
    server.shutdown().await;
}

// ---- Activity: submit + status snapshot ----

#[tokio::test]
async fn greet_activity_status() {
    let server = TestServer::start(test_addr!(7)).await;
    let exec_id = server.generate_execution_id().await;

    let resp = server
        .submit_follow_with_id(
            &exec_id,
            "testing:integration/activity-greet.greet",
            vec![json!("World")],
        )
        .await;
    assert_eq!(resp.status().as_u16(), 201);
    // Consume the streamed body to wait for execution to finish.
    let _: Value = resp.json().await.unwrap();

    let status = server.get_status(&exec_id).await;
    let status = sanitize_json(&status);
    insta::assert_json_snapshot!("greet_activity_status", status);
    server.shutdown().await;
}

// ---- Workflow: submit + events + replay ----

#[tokio::test]
async fn submit_workflow_and_replay() {
    let server = TestServer::start(test_addr!(8)).await;
    let exec_id = server.generate_execution_id().await;

    let resp = server
        .submit_follow_with_id(
            &exec_id,
            "testing:integration/workflow-add.add-workflow",
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
    server.shutdown().await;
}

// ---- Workflow: submit activity via join set + getResult ----

#[tokio::test]
async fn submit_workflow_with_get_result() {
    let server = TestServer::start(test_addr!(9)).await;
    let exec_id = server.generate_execution_id().await;

    let resp = server
        .submit_follow_with_id(
            &exec_id,
            "testing:integration/workflow-add-via-activity.add-via-activity",
            vec![json!(7), json!(8)],
        )
        .await;
    assert_eq!(resp.status().as_u16(), 201);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body, json!({ "ok": "15" }));

    let events = server.get_events(&exec_id).await;
    let events = sanitize_json(&events);
    insta::assert_json_snapshot!("workflow_add_via_activity_events", events);
    server.shutdown().await;
}

// ---- Workflow: obelisk.call() convenience API ----

#[tokio::test]
async fn submit_workflow_with_call() {
    let server = TestServer::start(test_addr!(10)).await;
    let exec_id = server.generate_execution_id().await;

    let resp = server
        .submit_follow_with_id(
            &exec_id,
            "testing:integration/workflow-call-activity.call-activity",
            vec![json!(3), json!(4)],
        )
        .await;
    assert_eq!(resp.status().as_u16(), 201);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body, json!({ "ok": "7" }));

    let events = server.get_events(&exec_id).await;
    let events = sanitize_json(&events);
    insta::assert_json_snapshot!("workflow_call_activity_events", events);
    server.shutdown().await;
}

// ---- Execution listing ----

#[tokio::test]
async fn list_executions_after_submit() {
    let server = TestServer::start(test_addr!(11)).await;

    let resp = server
        .submit_follow(
            "testing:integration/activity-add.add",
            vec![json!(1), json!(2)],
        )
        .await;
    assert_eq!(resp.status().as_u16(), 201);
    let _: Value = resp.json().await.unwrap();

    let executions = server.list_executions().await;
    let arr = executions.as_array().expect("array");
    assert_eq!(arr.len(), 1, "unexpected {arr:?}");
    assert_eq!(
        arr[0]["ffqn"],
        json!("testing:integration/activity-add.add"),
        "unexpected {arr:?}"
    );
    server.shutdown().await;
}

// ---- Error cases ----

#[tokio::test]
async fn submit_with_wrong_params_returns_error() {
    let server = TestServer::start(test_addr!(12)).await;

    let resp = server
        .client
        .post(format!("{}/v1/executions", server.base_url))
        .header("Accept", "application/json")
        .json(&json!({
            "ffqn": "testing:integration/activity-add.add",
            "params": [1]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 400);
    server.shutdown().await;
}

#[tokio::test]
async fn submit_nonexistent_function_returns_404() {
    let server = TestServer::start(test_addr!(13)).await;

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
    server.shutdown().await;
}

#[tokio::test]
async fn replay_nonexistent_execution_returns_404() {
    let server = TestServer::start(test_addr!(14)).await;
    let resp = server.replay("E_01AAAAAAAAAAAAAAAAAAAAAAAA").await;
    assert_eq!(resp.status().as_u16(), 404);
    server.shutdown().await;
}

#[tokio::test]
async fn activity_js_fetch_denied() {
    let server = TestServer::start(test_addr!(15)).await;
    let param_url = format!("http://{}/v1/components", server.api_addr());
    let resp = server
        .submit_follow(
            "testing:integration/fetch-get-denied.fetch-get",
            vec![json!(param_url), json!([["accept", "application/json"]])],
        )
        .await;
    assert_eq!(resp.status().as_u16(), 201);
    let body: Value = resp.json().await.unwrap();
    let err = body["err"].as_str().expect("expected err field");
    assert!(
        err.contains("HttpRequestDenied"),
        "Expected error to contain 'HttpRequestDenied', got: {err}"
    );
    server.shutdown().await;
}

#[tokio::test]
async fn activity_js_fetch_allowed() {
    let server = TestServer::start(test_addr!(16)).await;
    let param_url = format!("http://{}/v1/components", server.api_addr());
    let resp = server
        .submit_follow(
            "testing:integration/fetch-get-allowed.fetch-get",
            vec![json!(param_url), json!([["accept", "application/json"]])],
        )
        .await;
    assert_eq!(resp.status().as_u16(), 201);
    let body: Value = resp.json().await.unwrap();
    let result = body["ok"].as_str().expect("expected ok field");
    debug!("result: {result}");
    // The response should be a JSON array of components
    let components: Value = serde_json::from_str(result).unwrap();
    assert!(components.is_array());
    server.shutdown().await;
}

#[tokio::test]
async fn activity_js_read_env() {
    let server = TestServer::start(test_addr!(17)).await;
    let resp = server
        .submit_follow(
            "testing:integration/activity-env.read-env",
            vec![json!("TEST_ENV_VAR")],
        )
        .await;
    assert_eq!(resp.status().as_u16(), 201);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body, json!({ "ok": "hello_from_env" }));
    server.shutdown().await;
}

#[tokio::test]
async fn activity_js_record_return_type() {
    let server = TestServer::start(test_addr!(24)).await;
    let resp = server
        .submit_follow(
            "testing:integration/activity-make-record.make-record",
            vec![json!("Alice")],
        )
        .await;
    assert_eq!(resp.status().as_u16(), 201);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body, json!({ "ok": { "name": "Alice", "count": 42 } }));
    server.shutdown().await;
}

#[tokio::test]
async fn activity_js_throw_null_void_err() {
    let server = TestServer::start(test_addr!(26)).await;
    let resp = server
        .submit_follow("testing:integration/activity-throw-null.throw-null", vec![])
        .await;
    assert_eq!(resp.status().as_u16(), 201);
    let body: Value = resp.json().await.unwrap();
    // `throw null` with void err channel → Err(None) → {"err": null}
    assert_eq!(body, json!({ "err": null }));
    server.shutdown().await;
}

#[tokio::test]
async fn activity_js_variant_err_throw() {
    let server = TestServer::start(test_addr!(25)).await;
    let resp = server
        .submit_follow(
            "testing:integration/activity-throw-variant.throw-variant",
            vec![],
        )
        .await;
    assert_eq!(resp.status().as_u16(), 201);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body, json!({ "err": "not_found" }));
    server.shutdown().await;
}

// ---- Workflow: rich return types ----

#[tokio::test]
async fn workflow_js_rich_return_type() {
    let server = TestServer::start(test_addr!(27)).await;

    // ok: record
    let resp = server
        .submit_follow(
            "testing:integration/workflow-make-record.make-record",
            vec![json!("Alice")],
        )
        .await;
    assert_eq!(resp.status().as_u16(), 201);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body, json!({ "ok": { "name": "Alice", "count": 42 } }));

    // err: variant case
    let resp = server
        .submit_follow(
            "testing:integration/workflow-throw-variant.throw-variant",
            vec![],
        )
        .await;
    assert_eq!(resp.status().as_u16(), 201);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body, json!({ "err": "not_found" }));

    // err: null (void err channel — result<string>)
    let resp = server
        .submit_follow("testing:integration/workflow-throw-null.throw-null", vec![])
        .await;
    assert_eq!(resp.status().as_u16(), 201);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body, json!({ "err": null }));
    server.shutdown().await;
}

// ---- Idempotency ----

#[tokio::test]
async fn idempotent_submit_same_execution_id() {
    let server = TestServer::start(test_addr!(18)).await;
    let exec_id = server.generate_execution_id().await;

    let resp1 = server
        .submit_follow_with_id(
            &exec_id,
            "testing:integration/activity-add.add",
            vec![json!(1), json!(2)],
        )
        .await;
    assert_eq!(resp1.status().as_u16(), 201);
    let body1: Value = resp1.json().await.unwrap();

    let resp2 = server
        .submit_follow_with_id(
            &exec_id,
            "testing:integration/activity-add.add",
            vec![json!(1), json!(2)],
        )
        .await;
    assert_eq!(resp2.status().as_u16(), 200);
    let body2: Value = resp2.json().await.unwrap();
    assert_eq!(body1, body2);
    server.shutdown().await;
}

// ---- Webhook JS ----

#[tokio::test]
async fn webhook_js_hello() {
    let server = TestServer::start(test_addr!(19)).await;
    let resp = server
        .client
        .get(format!("{}/hello", server.webhook_base_url))
        .send()
        .await
        .expect("webhook request failed");
    assert_eq!(resp.status().as_u16(), 200);
    let body = resp.text().await.unwrap();
    assert_eq!(body, "Hello from JS webhook!");
    server.shutdown().await;
}

#[tokio::test]
async fn webhook_js_request_headers() {
    let server = TestServer::start(test_addr!(20)).await;
    let resp = server
        .client
        .get(format!("{}/headers", server.webhook_base_url))
        .header("x-custom", "value1")
        .header("x-custom", "value2")
        .send()
        .await
        .expect("webhook request failed");
    assert_eq!(resp.status().as_u16(), 200);
    let body = resp.text().await.unwrap();
    let headers: Vec<String> = serde_json::from_str(&body).unwrap();
    assert_eq!(headers, vec!["value1", "value2"]);
    server.shutdown().await;
}

#[tokio::test]
async fn webhook_js_fetch_allowed() {
    let server = TestServer::start(test_addr!(21)).await;
    let resp = server
        .client
        .get(format!("{}/fetch-allowed", server.webhook_base_url))
        .header("x-target-addr", server.api_addr())
        .send()
        .await
        .expect("webhook request failed");
    assert_eq!(resp.status().as_u16(), 200);
    let body = resp.text().await.unwrap();
    // The response should be a JSON array of components
    let components: Value = serde_json::from_str(&body).unwrap();
    assert!(components.is_array());
    server.shutdown().await;
}

#[tokio::test]
async fn webhook_js_fetch_denied() {
    let server = TestServer::start(test_addr!(22)).await;
    let resp = server
        .client
        .get(format!("{}/fetch-denied", server.webhook_base_url))
        .header("x-target-addr", server.api_addr())
        .send()
        .await
        .expect("webhook request failed");
    assert_eq!(resp.status().as_u16(), 500);
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("HttpRequestDenied"),
        "Expected body to contain 'HttpRequestDenied', got: {body}"
    );
    server.shutdown().await;
}

#[tokio::test]
async fn webhook_js_call_activity() {
    let server = TestServer::start(test_addr!(23)).await;
    let resp = server
        .client
        .get(format!("{}/call-activity/5/7", server.webhook_base_url))
        .send()
        .await
        .expect("webhook request failed");
    assert_eq!(resp.status().as_u16(), 200);
    let body: Value = resp.json().await.unwrap();
    // The add activity returns the sum as a string
    assert_eq!(body["result"], "12");
    server.shutdown().await;
}

#[tokio::test]
async fn webhook_js_env_var() {
    let server = TestServer::start(test_addr!(29)).await;
    let resp = server
        .client
        .get(format!("{}/read-env", server.webhook_base_url))
        .send()
        .await
        .expect("webhook request failed");
    assert_eq!(resp.status().as_u16(), 200);
    let body = resp.text().await.unwrap();
    assert_eq!(body, "hello_from_webhook_env");
    server.shutdown().await;
}

// ---- Inline stub activity ----

#[tokio::test]
async fn inline_stub_self_stubbing() {
    let server = TestServer::start(test_addr!(28)).await;
    let resp = server
        .submit_follow(
            "testing:integration/workflow-call-stub.call-stub",
            vec![json!(42u64)],
        )
        .await;
    assert_eq!(resp.status().as_u16(), 201);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body, json!({"ok": "stub-ok"}));
    server.shutdown().await;
}

/// Hot-redeploy an activity and verify the updated env var is visible immediately.
///
/// The `test_read_env_activity` JS activity reads an env var by name and returns
/// its value.  The initial deployment sets `TEST_ENV_VAR=hello_from_env`.  A
/// second deployment changes that value to `updated_value`.  After a hot redeploy
/// the activity must return the new value without a server restart.
#[tokio::test]
async fn hot_redeploy_activity() {
    use chrono::Utc;
    use concepts::prefixed_ulid::DeploymentId;
    use concepts::storage::{
        DbPool as _, DbPoolCloseable as _, DeploymentRecord, DeploymentStatus,
    };
    use db_sqlite::sqlite_dao::{SqliteConfig, SqlitePool};
    use grpc::grpc_gen::{
        DeploymentId as GrpcDeploymentId, SwitchDeploymentRequest,
        deployment_repository_client::DeploymentRepositoryClient,
        switch_deployment_response::Outcome,
    };

    let server = TestServer::start(test_addr!(30)).await;

    // 1. Run the activity with the initial deployment — must return the configured value.
    let resp = server
        .submit_follow(
            "testing:integration/activity-env.read-env",
            vec![json!("TEST_ENV_VAR")],
        )
        .await;
    assert_eq!(resp.status().as_u16(), 201);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body, json!({"ok": "hello_from_env"}));

    // 2. Build a second deployment with the env var changed to "updated_value".
    let second_id = DeploymentId::generate();
    {
        let pool = SqlitePool::new(&server.sqlite_file, SqliteConfig::default())
            .await
            .unwrap();
        let conn = pool.external_api_conn().await.unwrap();
        let active = conn.get_active_deployment().await.unwrap().unwrap();

        let config: serde_json::Value = serde_json::from_str(&active.config_json).unwrap();
        let mut new_deployment: DeploymentCanonical = serde_json::from_value(config).unwrap();
        let found = new_deployment
            .activities_js
            .iter_mut()
            .find(|activity| &**activity.name == "test_read_env_activity")
            .unwrap();
        found.env_vars = vec![EnvVarConfig::KeyValue {
            key: "TEST_ENV_VAR".to_string(),
            value: "updated_value".to_string(),
        }];

        let new_config_json = crate::config::toml::compute_config_json(&new_deployment);

        let now = Utc::now();
        conn.insert_deployment(DeploymentRecord {
            deployment_id: second_id,
            created_at: now,
            last_active_at: None,
            status: DeploymentStatus::Inactive,
            config_json: new_config_json,
            obelisk_version: crate::args::shadow::PKG_VERSION.to_string(),
            created_by: Some("test".to_string()),
        })
        .await
        .unwrap();
        pool.close().await;
    }

    // 3. Hot-redeploy to the second deployment.
    let mut client = DeploymentRepositoryClient::connect(format!("http://{}", server.api_addr()))
        .await
        .unwrap();
    let resp = client
        .switch_deployment(SwitchDeploymentRequest {
            deployment_id: Some(GrpcDeploymentId {
                id: second_id.to_string(),
            }),
            verify: false,
            hot_redeploy: true,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.outcome(), Outcome::SwitchOutcomeSwitched);

    // 4. Run the activity again — must return the updated value.
    let resp = server
        .submit_follow(
            "testing:integration/activity-env.read-env",
            vec![json!("TEST_ENV_VAR")],
        )
        .await;
    assert_eq!(resp.status().as_u16(), 201);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body, json!({"ok": "updated_value"}));

    server.shutdown().await;
}

/// After a hot redeploy, both the gRPC server and the web API server must expose
/// the updated component registry.
///
/// A new inline stub `testing:integration/stubs.new-hot-stub` is added only in
/// the second deployment.  The test asserts that before the hot redeploy the stub
/// is absent from both REST `/v1/functions` and gRPC `ListComponents`, and present
/// in both after it.
#[tokio::test]
async fn hot_redeploy_registry() {
    const NEW_STUB_FFQN: &str = "testing:integration/stubs.new-hot-stub";

    let server = TestServer::start(test_addr!(31)).await;
    let grpc_endpoint = format!("http://{}", server.api_addr());

    // Helper: check whether the new stub ffqn appears in REST /v1/functions.
    let rest_has_new_stub = || async {
        let functions = server
            .client
            .get(format!("{}/v1/functions", server.base_url))
            .header("Accept", "application/json")
            .send()
            .await
            .unwrap()
            .json::<Value>()
            .await
            .unwrap();
        functions
            .as_array()
            .unwrap()
            .iter()
            .any(|f| f["ffqn"] == NEW_STUB_FFQN)
    };

    // Helper: check whether the new stub appears in gRPC ListComponents exports.
    let grpc_has_new_stub = |endpoint: String| async move {
        let mut fn_client = FunctionRepositoryClient::connect(endpoint).await.unwrap();
        let resp = fn_client
            .list_components(ListComponentsRequest {
                function_name: None,
                component_digest: None,
                extensions: false,
            })
            .await
            .unwrap()
            .into_inner();
        resp.components.iter().any(|c| {
            c.exports.iter().any(|f| {
                f.function_name
                    .as_ref()
                    .is_some_and(|n| n.function_name == "new-hot-stub")
            })
        })
    };

    // Confirm the new stub is absent before the hot redeploy.
    assert!(
        !rest_has_new_stub().await,
        "stub must be absent before hot redeploy (REST)"
    );
    assert!(
        !grpc_has_new_stub(grpc_endpoint.clone()).await,
        "stub must be absent before hot redeploy (gRPC)"
    );

    // Build a second deployment that adds the new inline stub.
    let second_id = DeploymentId::generate();
    {
        let pool = SqlitePool::new(&server.sqlite_file, SqliteConfig::default())
            .await
            .unwrap();
        let conn = pool.external_api_conn().await.unwrap();
        let active = conn.get_active_deployment().await.unwrap().unwrap();

        let config: serde_json::Value = serde_json::from_str(&active.config_json).unwrap();
        let mut new_deployment: DeploymentCanonical = serde_json::from_value(config).unwrap();
        new_deployment
            .activities_stub
            .push(ActivityStubComponentConfigToml::Inline(
                ActivityStubInlineConfigToml {
                    name: ConfigName::new(concepts::StrVariant::Static("new_hot_stub")).unwrap(),
                    ffqn: NEW_STUB_FFQN.parse().unwrap(),
                    params: Some(vec![]),
                    return_type: Some("result<string, string>".to_string()),
                },
            ));

        let new_config_json = crate::config::toml::compute_config_json(&new_deployment);

        let now = Utc::now();
        conn.insert_deployment(DeploymentRecord {
            deployment_id: second_id,
            created_at: now,
            last_active_at: None,
            status: DeploymentStatus::Inactive,
            config_json: new_config_json,
            obelisk_version: crate::args::shadow::PKG_VERSION.to_string(),
            created_by: Some("test".to_string()),
        })
        .await
        .unwrap();
        pool.close().await;
    }

    // Hot-redeploy to the second deployment.
    let mut deploy_client = DeploymentRepositoryClient::connect(grpc_endpoint.clone())
        .await
        .unwrap();
    let resp = deploy_client
        .switch_deployment(SwitchDeploymentRequest {
            deployment_id: Some(GrpcDeploymentId {
                id: second_id.to_string(),
            }),
            verify: false,
            hot_redeploy: true,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.outcome(), Outcome::SwitchOutcomeSwitched);

    // Both servers must now expose the updated registry.
    assert!(
        rest_has_new_stub().await,
        "stub must be present after hot redeploy (REST)"
    );
    assert!(
        grpc_has_new_stub(grpc_endpoint).await,
        "stub must be present after hot redeploy (gRPC)"
    );

    server.shutdown().await;
}

/// After a hot redeploy, an existing JS webhook endpoint must pick up the new
/// env-var value from the updated deployment — proving that `WebhookServerState`
/// (including `fn_registry`, `deployment_id`, and the rebuilt router) is pushed
/// through the `WebhookRegistry` watch channel.
#[tokio::test]
async fn hot_redeploy_webhook_js_env_var() {
    let server = TestServer::start(test_addr!(32)).await;

    // 1. Verify the initial env var value is served.
    let resp = server
        .client
        .get(format!("{}/read-env", server.webhook_base_url))
        .send()
        .await
        .expect("webhook request failed");
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(resp.text().await.unwrap(), "hello_from_webhook_env");

    // 2. Build a second deployment with the env var changed.
    let second_id = DeploymentId::generate();
    {
        let pool = SqlitePool::new(&server.sqlite_file, SqliteConfig::default())
            .await
            .unwrap();
        let conn = pool.external_api_conn().await.unwrap();
        let active = conn.get_active_deployment().await.unwrap().unwrap();

        let mut new_deployment: DeploymentCanonical =
            serde_json::from_str(&active.config_json).unwrap();
        let found = new_deployment
            .webhooks_js
            .iter_mut()
            .find(|w| &**w.name == "test_read_env_webhook")
            .unwrap();
        found.env_vars = vec![EnvVarConfig::KeyValue {
            key: "WEBHOOK_TEST_ENV_VAR".to_string(),
            value: "updated_webhook_env".to_string(),
        }];

        let new_config_json = crate::config::toml::compute_config_json(&new_deployment);
        conn.insert_deployment(DeploymentRecord {
            deployment_id: second_id,
            created_at: Utc::now(),
            last_active_at: None,
            status: DeploymentStatus::Inactive,
            config_json: new_config_json,
            obelisk_version: crate::args::shadow::PKG_VERSION.to_string(),
            created_by: Some("test".to_string()),
        })
        .await
        .unwrap();
        pool.close().await;
    }

    // 3. Hot-redeploy to the second deployment.
    let mut client = DeploymentRepositoryClient::connect(format!("http://{}", server.api_addr()))
        .await
        .unwrap();
    let resp = client
        .switch_deployment(SwitchDeploymentRequest {
            deployment_id: Some(GrpcDeploymentId {
                id: second_id.to_string(),
            }),
            verify: false,
            hot_redeploy: true,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.outcome(), Outcome::SwitchOutcomeSwitched);

    // 4. The webhook must now return the updated env var.
    // Use a fresh client to ensure a new TCP connection is made (the per-connection
    // state snapshot means a keep-alive connection would still see the old state).
    let fresh_client = reqwest::Client::new();
    let resp = fresh_client
        .get(format!("{}/read-env", server.webhook_base_url))
        .send()
        .await
        .expect("webhook request failed after hot redeploy");
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(resp.text().await.unwrap(), "updated_webhook_env");

    server.shutdown().await;
}

/// After a hot redeploy that removes a JS webhook endpoint, the route must
/// return 404 — proving that the router inside the running HTTP server is
/// replaced, not just the env vars.
#[tokio::test]
async fn hot_redeploy_webhook_js_remove_endpoint() {
    let server = TestServer::start(test_addr!(33)).await;

    // 1. Verify /hello is served by the initial deployment.
    let resp = server
        .client
        .get(format!("{}/hello", server.webhook_base_url))
        .send()
        .await
        .expect("webhook request failed");
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(resp.text().await.unwrap(), "Hello from JS webhook!");

    // 2. Build a second deployment that removes test_hello_webhook.
    let second_id = DeploymentId::generate();
    {
        let pool = SqlitePool::new(&server.sqlite_file, SqliteConfig::default())
            .await
            .unwrap();
        let conn = pool.external_api_conn().await.unwrap();
        let active = conn.get_active_deployment().await.unwrap().unwrap();

        let mut new_deployment: DeploymentCanonical =
            serde_json::from_str(&active.config_json).unwrap();
        new_deployment
            .webhooks_js
            .retain(|w| &**w.name != "test_hello_webhook");

        let new_config_json = crate::config::toml::compute_config_json(&new_deployment);
        conn.insert_deployment(DeploymentRecord {
            deployment_id: second_id,
            created_at: Utc::now(),
            last_active_at: None,
            status: DeploymentStatus::Inactive,
            config_json: new_config_json,
            obelisk_version: crate::args::shadow::PKG_VERSION.to_string(),
            created_by: Some("test".to_string()),
        })
        .await
        .unwrap();
        pool.close().await;
    }

    // 3. Hot-redeploy to the second deployment.
    let mut client = DeploymentRepositoryClient::connect(format!("http://{}", server.api_addr()))
        .await
        .unwrap();
    let resp = client
        .switch_deployment(SwitchDeploymentRequest {
            deployment_id: Some(GrpcDeploymentId {
                id: second_id.to_string(),
            }),
            verify: false,
            hot_redeploy: true,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.outcome(), Outcome::SwitchOutcomeSwitched);

    // 4. /hello must now return 404 — the endpoint was removed from the router.
    // Use a fresh client to ensure a new TCP connection is made (the per-connection
    // state snapshot means a keep-alive connection would still see the old router).
    let fresh_client = reqwest::Client::new();
    let resp = fresh_client
        .get(format!("{}/hello", server.webhook_base_url))
        .send()
        .await
        .expect("request to removed webhook should still complete");
    assert_eq!(
        resp.status().as_u16(),
        404,
        "removed webhook endpoint must return 404 after hot redeploy"
    );

    server.shutdown().await;
}

// ---- crypto.subtle ----

#[tokio::test]
async fn activity_js_crypto_subtle_hmac_sign_verify() {
    const KEY: &str = "super-secret-key";
    const MSG: &str = "hello world";

    let server = TestServer::start(test_addr!(34)).await;
    let resp = server
        .submit_follow(
            "testing:integration/activity-hmac.hmac-sign-verify",
            vec![json!(KEY), json!(MSG)],
        )
        .await;
    assert_eq!(resp.status().as_u16(), 201);
    let body: Value = resp.json().await.unwrap();

    // The JS activity returns the HMAC-SHA256 signature as a hex string.
    let js_hex = body["ok"].as_str().expect("expected ok string");

    // Compute the expected HMAC-SHA256 on the Rust side and compare.
    let mut mac = Hmac::<Sha256>::new_from_slice(KEY.as_bytes()).unwrap();
    mac.update(MSG.as_bytes());
    let mut expected = String::with_capacity(64);
    for b in mac.finalize().into_bytes() {
        write!(expected, "{b:02x}").unwrap();
    }

    assert_eq!(js_hex, expected, "JS HMAC-SHA256 signature must match Rust");
    server.shutdown().await;
}

// ---- Backtrace API ----

#[tokio::test]
async fn backtrace_workflow_calling_activity() {
    let server = TestServer::start(test_addr!(35)).await;
    let exec_id = server.generate_execution_id().await;

    // Run a workflow that calls a child activity — backtrace is captured at the join-set call site.
    let resp = server
        .submit_follow_with_id(
            &exec_id,
            "testing:integration/workflow-add-via-activity.add-via-activity",
            vec![json!(3), json!(4)],
        )
        .await;
    assert_eq!(resp.status().as_u16(), 201);
    let _: Value = resp.json().await.unwrap(); // consume body

    // Default (last) filter.
    let resp = server.get_backtrace(&exec_id, None).await;
    assert_eq!(resp.status().as_u16(), 200, "backtrace should exist");
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["execution_id"], json!(exec_id));
    assert!(
        body["component_id"].as_str().is_some_and(|s| !s.is_empty()),
        "component_id must be a non-empty string"
    );
    assert!(
        body["version_min_including"].is_number(),
        "version_min_including must be a number"
    );
    assert!(
        body["version_max_excluding"].is_number(),
        "version_max_excluding must be a number"
    );
    assert!(
        body["wasm_backtrace"]["frames"].is_array(),
        "wasm_backtrace.frames must be an array"
    );

    // ?filter=first should also succeed and return a consistent structure.
    let resp_first = server.get_backtrace(&exec_id, Some("first")).await;
    assert_eq!(
        resp_first.status().as_u16(),
        200,
        "filter=first should work"
    );
    let body_first: Value = resp_first.json().await.unwrap();
    assert_eq!(body_first["execution_id"], json!(exec_id));

    // ?filter=<version_min_including> (numeric) should return the same record.
    let version_num = body["version_min_including"].as_u64().unwrap();
    let resp_num = server
        .get_backtrace(&exec_id, Some(&version_num.to_string()))
        .await;
    assert_eq!(
        resp_num.status().as_u16(),
        200,
        "numeric filter matching stored version should work"
    );

    // Invalid filter string must return 400.
    let resp_bad = server.get_backtrace(&exec_id, Some("bogus")).await;
    assert_eq!(
        resp_bad.status().as_u16(),
        400,
        "invalid filter must be 400"
    );

    // Non-existent (but well-formed) execution ID must return 404.
    let resp_missing = server
        .get_backtrace("E_01AAAAAAAAAAAAAAAAAAAAAAAA", None)
        .await;
    assert_eq!(
        resp_missing.status().as_u16(),
        404,
        "unknown execution must be 404"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn backtrace_source_workflow_calling_activity() {
    let server = TestServer::start(test_addr!(36)).await;
    let exec_id = server.generate_execution_id().await;

    // Run the workflow to ensure it has an associated component digest in the backtrace table.
    let resp = server
        .submit_follow_with_id(
            &exec_id,
            "testing:integration/workflow-add-via-activity.add-via-activity",
            vec![json!(2), json!(3)],
        )
        .await;
    assert_eq!(resp.status().as_u16(), 201);
    let _: Value = resp.json().await.unwrap();

    // The deployment config registers "add_via_activity.js" as an exact-key source for this
    // workflow component.  The endpoint resolves source by component digest (from the backtrace)
    // plus the file query param.
    let resp = server
        .get_backtrace_source(&exec_id, "add_via_activity.js", None)
        .await;
    assert_eq!(
        resp.status().as_u16(),
        200,
        "registered source file must be retrievable"
    );
    let body: Value = resp.json().await.unwrap();
    let source = body.as_str().expect("source content must be a JSON string");
    assert!(
        source.contains("createJoinSet"),
        "source must contain JS workflow content"
    );

    // ?filter=first should resolve the same component and return the same source.
    let resp_first = server
        .get_backtrace_source(&exec_id, "add_via_activity.js", Some("first"))
        .await;
    assert_eq!(resp_first.status().as_u16(), 200);
    let body_first: Value = resp_first.json().await.unwrap();
    assert_eq!(body_first, body, "filter=first must return the same source");

    // A file name not registered must return 404.
    let resp_missing = server
        .get_backtrace_source(&exec_id, "nonexistent_file.js", None)
        .await;
    assert_eq!(
        resp_missing.status().as_u16(),
        404,
        "unregistered source file must be 404"
    );

    server.shutdown().await;
}

// ---- Workflow: Math.random() sanity check ----

#[tokio::test]
async fn workflow_math_random() {
    let server = TestServer::start(test_addr!(37)).await;
    let exec_id = server.generate_execution_id().await;

    let resp = server
        .submit_follow_with_id(
            &exec_id,
            "testing:integration/workflow-math-random.math-random",
            vec![],
        )
        .await;
    assert_eq!(resp.status().as_u16(), 201);
    let body: Value = resp.json().await.unwrap();
    let result: Value = serde_json::from_str(body["ok"].as_str().unwrap()).unwrap();
    assert_eq!(
        json!(true),
        result["inRange"],
        "all random values must be in [0, 1): {result}"
    );

    // Replay must return the same result — random values are deterministic
    let replay_resp = server.replay(&exec_id).await;
    assert_eq!(replay_resp.status().as_u16(), 200);

    server.shutdown().await;
}

// ---- Workflow: Date.now() sanity check ----

#[tokio::test]
async fn workflow_date_now() {
    let server = TestServer::start(test_addr!(38)).await;
    let exec_id = server.generate_execution_id().await;

    let resp = server
        .submit_follow_with_id(
            &exec_id,
            "testing:integration/workflow-date-now.date-now",
            vec![],
        )
        .await;
    assert_eq!(resp.status().as_u16(), 201);
    let body: Value = resp.json().await.unwrap();
    let result: Value = serde_json::from_str(body["ok"].as_str().unwrap()).unwrap();
    assert_eq!(
        json!(true),
        result["isNumber"],
        "Date.now() must return a number: {result}"
    );

    // Replay must produce the same result — Date.now() uses the persisted clock
    let replay_resp = server.replay(&exec_id).await;
    assert_eq!(replay_resp.status().as_u16(), 200);

    server.shutdown().await;
}
