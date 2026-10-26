//! Integration tests for the Web API.
//!
//! Each test spins up a full Obelisk server (compile → DB → HTTP) against a
//! temporary TOML config that references the JS test-program fixtures.
//!
//! Each test uses a unique loopback address in the 127.1.0.0/16 range with
//! fixed ports, allowing parallel test execution without conflicts.
//! The `test_addr!` macro ensures unique addresses at link time.

use crate::command::server::{self, JsRuntimeMode, ServerAuth};
use crate::config::env_var::StartupEnvVars;
use crate::{
    command::server::{LocalDeployment, PrepareDirsParams, RunParams, prepare_dirs, run_internal},
    config::config_holder::ConfigHolder,
};
use rest_types::replay::{ReplayOutcomeSer, ReplayResponseSer};
use toml_edit::{DocumentMut, value};

mod activity_exec;
mod activity_vm;
mod crypto;
mod follow;
mod greet_activity;
mod multifile;
mod oidc;
mod util;
mod v8_heap;
mod webhook_js;
mod workflow_cancellation;

/// Append an inline `[[activity_stub]]` to a deployment manifest, mirroring the resolved
/// `ActivityStubExtInlineConfigResolved` the tests used to construct in-memory.
fn append_inline_stub(doc: &mut DocumentMut, name: &str, ffqn: &str) {
    let mut table = toml_edit::Table::new();
    table["name"] = value(name);
    table["ffqn"] = value(ffqn);
    table["params"] = toml_edit::Item::Value(toml_edit::Array::new().into());
    table["return_type"] = value("result<string, string>");
    doc.entry("activity_stub")
        .or_insert(toml_edit::Item::ArrayOfTables(
            toml_edit::ArrayOfTables::new(),
        ))
        .as_array_of_tables_mut()
        .expect("activity_stub must be an array of tables")
        .push(table);
}

/// Find the `[[<section>]]` table named `name` for in-place mutation.
fn manifest_component_mut<'a>(
    doc: &'a mut DocumentMut,
    section: &str,
    name: &str,
) -> &'a mut toml_edit::Table {
    doc.get_mut(section)
        .and_then(toml_edit::Item::as_array_of_tables_mut)
        .unwrap_or_else(|| panic!("manifest has no `{section}` section"))
        .iter_mut()
        .find(|table| table.get("name").and_then(toml_edit::Item::as_str) == Some(name))
        .unwrap_or_else(|| panic!("manifest has no `{section}` component named `{name}`"))
}

/// Set a component's `env_vars` to a single `{ key, value }` entry.
fn set_component_env_var(doc: &mut DocumentMut, section: &str, name: &str, key: &str, val: &str) {
    let mut entry = toml_edit::InlineTable::new();
    entry.insert("key", toml_edit::Value::from(key));
    entry.insert("value", toml_edit::Value::from(val));
    let mut arr = toml_edit::Array::new();
    arr.push(toml_edit::Value::InlineTable(entry));
    manifest_component_mut(doc, section, name)["env_vars"] = toml_edit::Item::Value(arr.into());
}

/// Remove the `[[<section>]]` table named `name` from the manifest.
fn remove_component(doc: &mut DocumentMut, section: &str, name: &str) {
    doc.get_mut(section)
        .and_then(toml_edit::Item::as_array_of_tables_mut)
        .unwrap_or_else(|| panic!("manifest has no `{section}` section"))
        .retain(|table| table.get("name").and_then(toml_edit::Item::as_str) != Some(name));
}
use concepts::FunctionFqn;
use concepts::prefixed_ulid::DeploymentId;
use concepts::storage::DbPool as _;
use concepts::storage::DbPoolCloseable;
use db_sqlite::sqlite_dao::{SqliteConfig, SqlitePool};
use directories::BaseDirs;
use hmac::{Hmac, KeyInit as _, Mac};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Sha256, Sha512};
use std::fmt::Write as _;
use std::{
    path::{Path, PathBuf},
    time::Duration,
};
use test_utils::sanitize_json;
use tokio::{sync::watch, task::JoinHandle};
use tracing::{debug, info, instrument};

struct FollowResponse {
    status: reqwest::StatusCode,
    data: String,
}

impl FollowResponse {
    async fn from_sse(mut response: reqwest::Response) -> Self {
        let status = response.status();
        assert_eq!(
            response.headers()[reqwest::header::CONTENT_TYPE],
            "text/event-stream"
        );
        let mut buffer = Vec::new();
        loop {
            let chunk = response
                .chunk()
                .await
                .expect("follow stream read failed")
                .expect("follow stream ended without a result");
            buffer.extend_from_slice(&chunk);
            if let Some(end) = buffer.windows(2).position(|pair| pair == b"\n\n") {
                let frame = std::str::from_utf8(&buffer[..end]).expect("SSE frame must be UTF-8");
                if let Some(data) = frame.lines().find_map(|line| line.strip_prefix("data: ")) {
                    return Self {
                        status,
                        data: data.to_string(),
                    };
                }
                buffer.drain(..end + 2);
            }
        }
    }

    fn status(&self) -> reqwest::StatusCode {
        self.status
    }

    fn json<T: serde::de::DeserializeOwned>(
        self,
    ) -> impl std::future::Future<Output = Result<T, serde_json::Error>> {
        std::future::ready(serde_json::from_str(&self.data))
    }
}

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

fn copy_dir_recursive(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).unwrap();
    for entry in std::fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());
        let file_type = entry.file_type().unwrap();
        if file_type.is_dir() {
            copy_dir_recursive(&src_path, &dst_path);
        } else if file_type.is_file() {
            std::fs::copy(&src_path, &dst_path).unwrap();
        }
    }
}

/// Fixed ports used by all integration tests.
/// Each test uses a unique IP address in 127.1.0.0/16, so ports don't conflict.
const API_PORT: u16 = 9080;
const WEBHOOK_PORT: u16 = 9081;

#[derive(Clone, Copy, Debug)]
enum WorkflowJsTestRuntime {
    BoaWasm,
    V8,
}

impl WorkflowJsTestRuntime {
    fn mode(self) -> JsRuntimeMode {
        match self {
            Self::BoaWasm => JsRuntimeMode::BoaWasm,
            Self::V8 => JsRuntimeMode::V8,
        }
    }

    fn ip(self, ip: String) -> String {
        match self {
            Self::BoaWasm => ip,
            Self::V8 => ip.replacen("127.1.", "127.2.", 1),
        }
    }
}

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

#[derive(Clone, Copy, Debug)]
enum JsRuntime {
    BoaWasm,
    V8,
}

impl JsRuntime {
    fn name(self) -> &'static str {
        match self {
            Self::BoaWasm => "boa_wasm",
            Self::V8 => "v8",
        }
    }

    fn mode(self) -> JsRuntimeMode {
        match self {
            Self::BoaWasm => JsRuntimeMode::BoaWasm,
            Self::V8 => JsRuntimeMode::V8,
        }
    }
}

/// Write separate server and deployment TOML configs to temp files and return their paths.
/// The server config includes API, DB, and wasm settings.
/// The deployment config references the JS fixtures from the workspace tree.
fn write_test_configs(
    ip: &str,
    server_toml_api_lines: &str,
) -> (tempfile::TempDir, PathBuf, PathBuf) {
    let workspace = get_workspace_dir();
    let (db_dir, server_path, deployment_path) =
        util::write_server_config(ip, server_toml_api_lines, "");
    let fixture_src = workspace.join("crates/testing/test-programs");
    let fixture_dst = db_dir.path().join("crates/testing/test-programs");
    copy_dir_recursive(&fixture_src.join("js"), &fixture_dst.join("js"));
    copy_dir_recursive(&fixture_src.join("exec"), &fixture_dst.join("exec"));
    let ws = ".";
    let deployment_contents = format!(
        r#"
[[activity_js]]
name = "test_add_activity"
location = "{ws}/crates/testing/test-programs/js/activity/add.js"
ffqn = "testing:integration/activity.add"
params = [
  {{ name = "a", type = "u32" }},
  {{ name = "b", type = "u32" }},
]
return_type = "result<u32, string>"

[[activity_js]]
name = "test_fetch_denied_activity"
location = "{ws}/crates/testing/test-programs/js/activity/fetch_get.js"
ffqn = "testing:integration/fetch-get-denied.fetch-get"
params = [
  {{ name = "url", type = "string" }},
  {{ name = "headers", type = "list<tuple<string,string>>" }},
]
return_type = "result<string, string>"

[[activity_js]]
name = "test_fetch_allowed_activity"
location = "{ws}/crates/testing/test-programs/js/activity/fetch_get.js"
ffqn = "testing:integration/fetch-get-allowed.fetch-get"
params = [
  {{ name = "url", type = "string" }},
  {{ name = "headers", type = "list<tuple<string,string>>" }},
]
return_type = "result<string, string>"
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
env_vars = [{{key = "TEST_ENV_VAR", value = "hello_from_env"}}]

[[activity_js]]
name = "test_make_record_activity"
location = "{ws}/crates/testing/test-programs/js/activity/make_record.js"
ffqn = "testing:integration/activity-make-record.make-record"
params = [
  {{ name = "name", type = "string" }},
]
return_type = "result<record {{ name: string, count: u32 }}, string>"

[[activity_js]]
name = "test_throw_variant_activity"
location = "{ws}/crates/testing/test-programs/js/activity/throw_variant.js"
ffqn = "testing:integration/activity-throw-variant.throw-variant"
params = []
return_type = "result<u32, variant {{ execution-failed, not-found }}>"

[[activity_js]]
name = "test_throw_null_activity"
location = "{ws}/crates/testing/test-programs/js/activity/throw_null.js"
ffqn = "testing:integration/activity-throw-null.throw-null"
params = []
return_type = "result<string>"

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
name = "test_add_cancellable_workflow"
location = "{ws}/crates/testing/test-programs/js/workflow/add_workflow.js"
ffqn = "testing:integration/workflow-add.add-workflow-cancellable"
params = [
  {{ name = "a", type = "u32" }},
  {{ name = "b", type = "u32" }},
]
return_type = "result<string, string>"

[[workflow_js]]
name = "test_sleep_cancellable_workflow"
location = "{ws}/crates/testing/test-programs/js/workflow/sleep_cancellable.js"
ffqn = "testing:integration/workflow-sleep.sleep-cancellable"
params = []
return_type = "result<string, string>"

[[workflow_js]]
name = "test_add_via_activity_workflow"
location = "{ws}/crates/testing/test-programs/js/workflow/add_via_activity.js"
ffqn = "testing:integration/workflow-add-via-activity.add-via-activity"
params = [
  {{ name = "a", type = "u32" }},
  {{ name = "b", type = "u32" }},
]
return_type = "result<u32, string>"

[[workflow_js]]
name = "test_call_activity_workflow"
location = "{ws}/crates/testing/test-programs/js/workflow/call_activity.js"
ffqn = "testing:integration/workflow-call-activity.call-activity"
params = [
  {{ name = "a", type = "u32" }},
  {{ name = "b", type = "u32" }},
]
return_type = "result<u32, string>"

[[workflow_js]]
name = "test_import_call_activity_workflow"
location = "{ws}/crates/testing/test-programs/js/workflow/import_call_activity.js"
ffqn = "testing:integration/workflow-import-call-activity.call-activity"
params = [
  {{ name = "a", type = "u32" }},
  {{ name = "b", type = "u32" }},
]
return_type = "result<u32, string>"

[[workflow_js]]
name = "test_import_star_call_activity_workflow"
location = "{ws}/crates/testing/test-programs/js/workflow/import_star_call_activity.js"
ffqn = "testing:integration/workflow-import-star-call-activity.call-activity"
params = [
  {{ name = "a", type = "u32" }},
  {{ name = "b", type = "u32" }},
]
return_type = "result<u32, string>"

[[workflow_js]]
name = "test_import_schedule_activity_workflow"
location = "{ws}/crates/testing/test-programs/js/workflow/import_schedule_activity.js"
ffqn = "testing:integration/workflow-import-schedule-activity.schedule-activity"
params = [
  {{ name = "a", type = "u32" }},
  {{ name = "b", type = "u32" }},
]
return_type = "result<string, string>"

[[workflow_js]]
name = "test_import_ext_activity_workflow"
location = "{ws}/crates/testing/test-programs/js/workflow/import_ext_activity.js"
ffqn = "testing:integration/workflow-import-ext-activity.add-via-activity"
params = [
  {{ name = "a", type = "u32" }},
  {{ name = "b", type = "u32" }},
]
return_type = "result<u32, string>"

[[workflow_js]]
name = "test_import_ext_submit_error_workflow"
location = "{ws}/crates/testing/test-programs/js/workflow/import_ext_submit_error.js"
ffqn = "testing:integration/workflow-import-ext-submit-error.submit-error"
params = []
return_type = "result<string, string>"

[[workflow_js]]
name = "test_import_stub_activity_workflow"
location = "{ws}/crates/testing/test-programs/js/workflow/import_stub_activity.js"
ffqn = "testing:integration/workflow-import-stub-activity.call-stub"
params = [
  {{ name = "id", type = "u64" }},
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
name = "test_join_next_try_semantics_workflow"
location = "{ws}/crates/testing/test-programs/js/workflow/join_next_try_semantics.js"
ffqn = "testing:integration/workflow-join-next-try-semantics.join-next-try-semantics"
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

[[workflow_js]]
name = "test_return_wrong_type_workflow"
location = "{ws}/crates/testing/test-programs/js/workflow/return_wrong_type.js"
ffqn = "testing:integration/workflow-return-wrong-type.return-wrong-type"
params = []
return_type = "result<u32>"

[[activity_exec]]
ffqn = "testing:integration/exec-greet.greet-inline"
content = '''
#!/usr/bin/env bash
set -exuo pipefail
raw=$(echo $1 | jq -r .)
echo "\"Hello, $raw!\""
'''
params = [
  {{ name = "name", type = "string" }},
]
return_type = "result<string, string>"
env_vars = ["PATH"] # for jq

[[activity_stub]]
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

[[webhook_endpoint_js]]
name = "test_generate_execution_id_webhook"
location = "{ws}/crates/testing/test-programs/js/webhook/generate_execution_id.js"
routes = [{{ methods = ["GET"], route = "/generate-execution-id" }}]

[[webhook_endpoint_js]]
name = "test_import_call_activity_webhook"
location = "{ws}/crates/testing/test-programs/js/webhook/import_call_activity.js"
routes = [{{ methods = ["GET"], route = "/import-call-activity" }}]

[[webhook_endpoint_js]]
name = "test_import_schedule_activity_webhook"
location = "{ws}/crates/testing/test-programs/js/webhook/import_schedule_activity.js"
routes = [{{ methods = ["GET"], route = "/import-schedule-activity" }}]

[[webhook_endpoint_js]]
name = "test_body_text_webhook"
location = "{ws}/crates/testing/test-programs/js/webhook/body_text.js"
routes = [{{ methods = ["POST"], route = "/body-text" }}]

[[webhook_endpoint_js]]
name = "test_body_json_webhook"
location = "{ws}/crates/testing/test-programs/js/webhook/body_json.js"
routes = [{{ methods = ["POST"], route = "/body-json" }}]

[[webhook_endpoint_js]]
name = "test_body_form_data_webhook"
location = "{ws}/crates/testing/test-programs/js/webhook/body_form_data.js"
routes = [{{ methods = ["POST"], route = "/body-form-data" }}]
"#,
    );
    debug!("Deployment TOML:{deployment_contents}");
    std::fs::write(&deployment_path, deployment_contents).unwrap();
    (db_dir, server_path, deployment_path)
}

async fn authorize_test_deployment(
    server_path: &std::path::Path,
    deployment_path: &std::path::Path,
) {
    test_utils::set_up();
    let registry = std::sync::Arc::new(
        crate::config::secret_registry::SecretRegistry::from_test_values([
            (
                "MY_SECRET".to_owned(),
                secrecy::SecretString::from("s3cret_value"),
            ),
            (
                "VM_SECRET".to_owned(),
                secrecy::SecretString::from("swordfish"),
            ),
        ])
        .with_public_env([
            "PATH".to_owned(),
            "OBELISK_PHASE5_DEFINITELY_MISSING_VAR".to_owned(),
        ]),
    );
    let outputs = crate::command::server::generate_secret_config_digests(
        deployment_path,
        None,
        registry,
        crate::parse_activity_vm_runtime_from_env(&StartupEnvVars::capture()).unwrap(),
    )
    .await
    .unwrap();
    crate::command::server::fix_app_digest_permissions(
        &server_path.with_file_name("app.toml"),
        &outputs,
        true,
        true,
    )
    .await
    .unwrap();
}

struct TestServer {
    ip: String,
    base_url: String,
    webhook_base_url: String,
    client: reqwest::Client,
    termination_sender: watch::Sender<()>,
    server_handle: JoinHandle<anyhow::Result<()>>,
    sqlite_file: std::path::PathBuf,
    tmp_dir: tempfile::TempDir,
    persisted_logs: Option<tokio::sync::broadcast::Receiver<concepts::storage::LogInfoAppendRow>>,
}

impl TestServer {
    fn corrupt_child_request_params_hash(&self, execution_id: &str) {
        let connection = rusqlite::Connection::open(&self.sqlite_file).unwrap();
        let json: String = connection
            .query_row(
                "SELECT json_value FROM t_execution_log WHERE execution_id = ?1 AND history_event_type = 'join_set_request'",
                [execution_id],
                |row| row.get(0),
            )
            .unwrap();
        let mut event: Value = serde_json::from_str(&json).unwrap();
        let params_hash = event
            .pointer_mut("/history_event/event/request/params_hash")
            .expect("child request params_hash must be present");
        *params_hash =
            json!("sha256:0000000000000000000000000000000000000000000000000000000000000000");
        connection
            .execute(
                "UPDATE t_execution_log SET json_value = ?1 WHERE execution_id = ?2 AND history_event_type = 'join_set_request'",
                (serde_json::to_string(&event).unwrap(), execution_id),
            )
            .unwrap();
    }

    async fn start(ip: String) -> Self {
        let (tmp_dir, server_path, deployment_path) = write_test_configs(&ip, "");
        authorize_test_deployment(&server_path, &deployment_path).await;
        let deployment = LocalDeployment::from_path(&deployment_path).await.unwrap();
        Self::launch(
            ip,
            tmp_dir,
            server_path,
            deployment,
            true,
            None,
            JsRuntimeMode::BoaWasm,
        )
        .await
    }

    async fn start_with_server_lines(ip: String, server_toml_lines: &str) -> Self {
        let (tmp_dir, server_path, deployment_path) = write_test_configs(&ip, server_toml_lines);
        authorize_test_deployment(&server_path, &deployment_path).await;
        let deployment = LocalDeployment::from_path(&deployment_path).await.unwrap();
        Self::launch(
            ip,
            tmp_dir,
            server_path,
            deployment,
            true,
            None,
            JsRuntimeMode::BoaWasm,
        )
        .await
    }

    async fn start_with_js_runtime(ip: String, js_runtime: JsRuntimeMode) -> Self {
        let (tmp_dir, server_path, deployment_path) = write_test_configs(&ip, "");
        authorize_test_deployment(&server_path, &deployment_path).await;
        let deployment = LocalDeployment::from_path(&deployment_path).await.unwrap();
        Self::launch(ip, tmp_dir, server_path, deployment, true, None, js_runtime).await
    }

    /// Launch a server whose deployment is exactly `deployment_toml`, letting a self-contained
    /// test carry its own manifest instead of the shared `write_test_configs` fixtures.
    /// `server_toml_tail` is appended to the generated `server.toml`; `files` are written into
    /// the deployment directory (e.g. a script referenced from a `location`).
    async fn start_inline_deployment(
        ip: String,
        server_toml_tail: &str,
        deployment_toml: &str,
        files: &[(&str, &str)],
    ) -> Self {
        Self::start_inline_deployment_with_js_runtime(
            ip,
            server_toml_tail,
            deployment_toml,
            files,
            JsRuntimeMode::BoaWasm,
        )
        .await
    }

    async fn start_inline_deployment_with_js_runtime(
        ip: String,
        server_toml_tail: &str,
        deployment_toml: &str,
        files: &[(&str, &str)],
        js_runtime: JsRuntimeMode,
    ) -> Self {
        Self::start_inline_deployment_with_hooks(
            ip,
            server_toml_tail,
            deployment_toml,
            files,
            js_runtime,
            server::RuntimeTestHooks::default(),
        )
        .await
    }

    async fn start_inline_deployment_with_hooks(
        ip: String,
        server_toml_tail: &str,
        deployment_toml: &str,
        files: &[(&str, &str)],
        js_runtime: JsRuntimeMode,
        test_hooks: server::RuntimeTestHooks,
    ) -> Self {
        let (tmp_dir, server_path, deployment_path) =
            util::write_server_config(&ip, "", server_toml_tail);
        for (rel, content) in files {
            let path = tmp_dir.path().join(rel);
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).unwrap();
            }
            std::fs::write(path, content).unwrap();
        }
        std::fs::write(&deployment_path, deployment_toml).unwrap();
        authorize_test_deployment(&server_path, &deployment_path).await;
        let deployment = LocalDeployment::from_path(&deployment_path).await.unwrap();
        Box::pin(Self::launch_with_hooks(
            ip,
            tmp_dir,
            server_path,
            deployment,
            true,
            None,
            js_runtime,
            test_hooks,
        ))
        .await
    }

    async fn start_with_server_lines_and_component(
        ip: String,
        server_toml_lines: &str,
        section: &str,
        component_name: &str,
    ) -> Self {
        let (tmp_dir, server_path, deployment_path) = write_test_configs(&ip, server_toml_lines);
        let mut deployment_doc = std::fs::read_to_string(&deployment_path)
            .unwrap()
            .parse::<DocumentMut>()
            .unwrap();
        deployment_doc
            .as_table_mut()
            .retain(|key, _| key == section);
        deployment_doc[section]
            .as_array_of_tables_mut()
            .unwrap()
            .retain(|table| {
                ["name", "ffqn"].into_iter().any(|key| {
                    table.get(key).and_then(toml_edit::Item::as_str) == Some(component_name)
                })
            });
        std::fs::write(&deployment_path, deployment_doc.to_string()).unwrap();
        authorize_test_deployment(&server_path, &deployment_path).await;
        let deployment = LocalDeployment::from_path(&deployment_path).await.unwrap();
        Self::launch(
            ip,
            tmp_dir,
            server_path,
            deployment,
            true,
            None,
            JsRuntimeMode::BoaWasm,
        )
        .await
    }

    /// Start a server with an empty deployment (no components, no CAS blobs), so a later
    /// `deployment apply` exercises the upload-then-submit path from a clean store.
    async fn start_empty(ip: String) -> Self {
        let (tmp_dir, server_path, _deployment_path) = write_test_configs(&ip, "");
        Self::launch(
            ip,
            tmp_dir,
            server_path,
            LocalDeployment::empty(),
            true,
            None,
            JsRuntimeMode::BoaWasm,
        )
        .await
    }

    /// Start an empty server with API auth enabled (`no_auth = false`), accepting the
    /// given extra top-level `server.toml` lines (e.g. `api.token_hashes = [...]`).
    async fn start_empty_with_auth(
        ip: String,
        server_toml_api_lines: &str,
        api_token: Option<&str>,
    ) -> Self {
        let (tmp_dir, server_path, _deployment_path) =
            write_test_configs(&ip, server_toml_api_lines);
        Self::launch(
            ip,
            tmp_dir,
            server_path,
            LocalDeployment::empty(),
            false,
            api_token.map(secrecy::SecretString::from),
            JsRuntimeMode::BoaWasm,
        )
        .await
    }

    async fn launch(
        ip: String,
        tmp_dir: tempfile::TempDir,
        server_path: PathBuf,
        deployment: LocalDeployment,
        no_auth: bool,
        api_token: Option<secrecy::SecretString>,
        js_runtime: JsRuntimeMode,
    ) -> Self {
        Box::pin(Self::launch_with_hooks(
            ip,
            tmp_dir,
            server_path,
            deployment,
            no_auth,
            api_token,
            js_runtime,
            server::RuntimeTestHooks::default(),
        ))
        .await
    }

    #[expect(clippy::too_many_arguments)]
    async fn launch_with_hooks(
        ip: String,
        tmp_dir: tempfile::TempDir,
        server_path: std::path::PathBuf,
        deployment: LocalDeployment,
        no_auth: bool,
        api_token: Option<secrecy::SecretString>,
        js_runtime: JsRuntimeMode,
        test_hooks: server::RuntimeTestHooks,
    ) -> Self {
        let persisted_logs = test_hooks
            .persisted_logs
            .as_ref()
            .map(tokio::sync::broadcast::Sender::subscribe);
        test_utils::set_up();

        let project_dirs = crate::project_dirs();
        let base_dirs = BaseDirs::new();
        let config_holder = ConfigHolder::new(project_dirs, base_dirs, Some(server_path.clone()))
            .unwrap()
            .with_app_source(Some(server_path.with_file_name("app.toml")))
            .unwrap();
        let mut config = config_holder.load_config().unwrap();
        let app = config_holder.load_app_config().unwrap();
        config.app_policy = app.policy();
        config.secrets = app.secrets;
        config.public_env = app.public_env;
        config.allowed_exec_activities = app.allowed_exec_activities;
        config.outbound_http = app.outbound_http;
        let secret_registry = std::sync::Arc::new(
            crate::config::secret_registry::SecretRegistry::from_test_values([
                (
                    "MY_SECRET".to_string(),
                    secrecy::SecretString::from("s3cret_value"),
                ),
                (
                    "VM_SECRET".to_string(),
                    secrecy::SecretString::from("swordfish"),
                ),
            ])
            .with_public_env([
                "PATH".to_string(),
                "OBELISK_PHASE5_DEFINITELY_MISSING_VAR".to_string(),
            ])
            .with_optional_public_env(["OBELISK_PHASE5_DEFINITELY_MISSING_VAR".to_string()])
            .with_exposure_config(&config.secrets),
        );

        let (termination_sender, termination_watcher) = watch::channel(());

        let params = RunParams {
            dir_params: PrepareDirsParams {
                clean_cache: false,
                clean_codegen_cache: false,
            },
            clean_sqlite_directory: false,
            suppress_type_checking_errors: false,
            auth: if no_auth {
                ServerAuth::NoAuth
            } else {
                ServerAuth::Auth { api_token }
            },
            js_runtime,
            activity_vm_runtime: crate::parse_activity_vm_runtime_from_env(
                &StartupEnvVars::capture(),
            )
            .unwrap(),
            test_hooks,
        };

        let prepared_dirs = prepare_dirs(
            &config,
            &params.dir_params,
            &config_holder.path_prefixes,
            &crate::config::secret_registry::SecretRegistry::empty(),
        )
        .await
        .unwrap();

        let base_url = format!("http://{ip}:{API_PORT}");
        let client = reqwest::Client::new();
        if client
            .get(format!("{base_url}/v1/functions"))
            .header("Accept", "application/json")
            .send()
            .await
            .is_ok()
        {
            panic!("{base_url} is reachable before server started");
        }

        let server_handle = tokio::spawn(async move {
            Box::pin(run_internal(
                config,
                Some(deployment),
                None,
                config_holder.path_prefixes,
                params,
                prepared_dirs,
                termination_watcher,
                secret_registry,
            ))
            .await
        });
        debug!("Spawned server task");

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
                && (resp.status().is_success()
                    || resp.status() == reqwest::StatusCode::UNAUTHORIZED)
            {
                debug!("Pinging server OK");
                break;
            }
            debug!("Pinging sever failed");
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        let webhook_base_url = format!("http://{ip}:{WEBHOOK_PORT}");
        let sqlite_file = tmp_dir.path().join(crate::config::server::SQLITE_FILE_NAME);
        TestServer {
            ip,
            base_url,
            webhook_base_url,
            client,
            termination_sender,
            server_handle,
            sqlite_file,
            tmp_dir,
            persisted_logs,
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

    async fn shutdown_with_timeout(self, timeout: Duration) {
        let Self {
            server_handle,
            termination_sender,
            ..
        } = self;
        drop(termination_sender);
        tokio::time::timeout(timeout, server_handle)
            .await
            .expect("server shutdown timed out")
            .expect("server task panicked")
            .expect("server shutdown failed");
    }

    async fn restart_with_persisted_value_limit(self, limit: i64) -> Self {
        let Self {
            ip,
            termination_sender,
            server_handle,
            tmp_dir,
            ..
        } = self;
        drop(termination_sender);
        let _ = server_handle.await;

        let server_path = tmp_dir.path().join("server.toml");
        let mut server_doc = std::fs::read_to_string(&server_path)
            .unwrap()
            .parse::<DocumentMut>()
            .unwrap();
        server_doc["limits"]["max_persisted_value_size_bytes"] = value(limit);
        std::fs::write(&server_path, server_doc.to_string()).unwrap();

        let deployment_path = tmp_dir.path().join("deployment.toml");
        authorize_test_deployment(&server_path, &deployment_path).await;
        let deployment = LocalDeployment::from_path(&deployment_path).await.unwrap();
        Self::launch(
            ip,
            tmp_dir,
            server_path,
            deployment,
            true,
            None,
            JsRuntimeMode::BoaWasm,
        )
        .await
    }

    // ---- helper methods ------------------------------------------------

    async fn submit_follow(&self, ffqn: &str, params: Vec<Value>) -> FollowResponse {
        let response = self
            .client
            .post(format!("{}/v1/executions?follow=true", self.base_url))
            .header("Accept", "text/event-stream")
            .json(&json!({ "ffqn": ffqn, "params": params }))
            .send()
            .await
            .expect("submit request failed");
        FollowResponse::from_sse(response).await
    }

    async fn submit_follow_with_id(
        &self,
        execution_id: &str,
        ffqn: &str,
        params: Vec<Value>,
    ) -> FollowResponse {
        let response = self
            .client
            .put(format!(
                "{}/v1/executions/{execution_id}?follow=true",
                self.base_url
            ))
            .header("Accept", "text/event-stream")
            .json(&json!({ "ffqn": ffqn, "params": params }))
            .send()
            .await
            .expect("submit request failed");
        FollowResponse::from_sse(response).await
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

    async fn get_logs(&self, execution_id: &str, length: usize) -> Value {
        self.client
            .get(format!(
                "{}/v1/executions/{execution_id}/logs?length={length}&direction=newer",
                self.base_url,
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

    async fn persist_backtraces(&self, execution_id: &str) -> u32 {
        let body: Value = self
            .client
            .put(format!(
                "{}/v1/executions/{execution_id}/backtrace/persist",
                self.base_url
            ))
            .header("Accept", "application/json")
            .send()
            .await
            .expect("persist-backtraces request failed")
            .json()
            .await
            .expect("persist-backtraces parse failed");
        body["persisted_backtrace_count"]
            .as_u64()
            .expect("persisted_backtrace_count must be a number")
            .try_into()
            .expect("persisted_backtrace_count must fit u32")
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
        self.list_components_for_deployment(None).await
    }

    async fn list_components_for_deployment(&self, deployment_id: Option<DeploymentId>) -> Value {
        let url = match deployment_id {
            Some(deployment_id) => format!(
                "{}/v1/components?deployment_id={deployment_id}",
                self.base_url
            ),
            None => format!("{}/v1/components", self.base_url),
        };
        let resp = self
            .client
            .get(url)
            .header("Accept", "application/json")
            .send()
            .await
            .expect("components request failed");
        let status = resp.status();
        let body = resp.text().await.expect("components body read failed");
        assert!(
            status.is_success(),
            "components request failed: {status} - {body}"
        );
        serde_json::from_str(&body).expect("components parse failed")
    }

    /// Read the active deployment's verbatim manifest from the database.
    async fn active_deployment_toml(&self) -> String {
        let pool = SqlitePool::new(&self.sqlite_file, SqliteConfig::default())
            .await
            .unwrap();
        let conn = pool.external_api_conn().await.unwrap();
        let active = conn.get_active_deployment().await.unwrap().unwrap();
        pool.close().await;
        active.deployment_toml
    }

    /// Take the active deployment's manifest, apply `mutate` to its TOML, and resubmit.
    /// The base manifest's file blobs are already in the CAS (uploaded at first activation),
    /// so no new uploads are needed for inline-only edits.
    async fn submit_modified_deployment(
        &self,
        mutate: impl FnOnce(&mut DocumentMut),
    ) -> DeploymentId {
        let mut doc = self
            .active_deployment_toml()
            .await
            .parse::<DocumentMut>()
            .unwrap();
        mutate(&mut doc);
        self.webapi_submit_deployment(&doc.to_string()).await
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
        version: Option<&str>,
    ) -> reqwest::Response {
        let version_part = match version {
            Some(v) => format!("&version={v}"),
            None => String::new(),
        };
        self.client
            .get(format!(
                "{}/v1/executions/{execution_id}/backtrace/source?file={file}{version_part}",
                self.base_url
            ))
            .header("Accept", "application/json")
            .send()
            .await
            .expect("backtrace/source request failed")
    }

    async fn get_backtrace(&self, execution_id: &str, version: Option<&str>) -> reqwest::Response {
        let url = match version {
            Some(v) => format!(
                "{}/v1/executions/{execution_id}/backtrace?version={v}",
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

    /// Submit a new deployment via the Web API and return its deployment ID.
    async fn webapi_submit_deployment(&self, deployment_toml: &str) -> DeploymentId {
        let resp = self
            .client
            .post(format!("{}/v1/deployments", self.base_url))
            .header("Accept", "application/json")
            .json(&json!({
                "deployment_toml": deployment_toml,
            }))
            .send()
            .await
            .expect("webapi submit deployment request failed");
        assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
        let location = resp
            .headers()
            .get(reqwest::header::LOCATION)
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        let body: Value = resp.json().await.unwrap();
        let deployment_id = body["deployment_id"].as_str().unwrap().parse().unwrap();
        assert_eq!(location, format!("/v1/deployments/{deployment_id}"));
        deployment_id
    }

    /// Submit a deployment via the Web API with an explicit (idempotency) deployment ID,
    /// returning the raw response so callers can assert on success or conflict.
    async fn webapi_submit_deployment_with_id(
        &self,
        deployment_toml: &str,
        deployment_id: DeploymentId,
    ) -> reqwest::Response {
        self.client
            .put(format!("{}/v1/deployments/{deployment_id}", self.base_url))
            .header("Accept", "application/json")
            .json(&json!({
                "deployment_toml": deployment_toml,
            }))
            .send()
            .await
            .expect("webapi submit deployment request failed")
    }

    /// Submit a deployment package as `multipart/form-data`. Each file is
    /// `(field name, path, bytes)`, the field name being the claimed digest or `file`.
    async fn webapi_submit_package(
        &self,
        deployment_id: DeploymentId,
        deployment_toml: &str,
        files: Vec<(String, String, Vec<u8>)>,
        allow_unavailable_runtime_config: bool,
    ) -> reqwest::Response {
        let mut form =
            reqwest::multipart::Form::new().text("deployment_toml", deployment_toml.to_string());
        if allow_unavailable_runtime_config {
            form = form.text("allow_unavailable_runtime_config", "true");
        }
        for (field, path, bytes) in files {
            let part = reqwest::multipart::Part::bytes(bytes)
                .file_name(path)
                .mime_str("application/octet-stream")
                .unwrap();
            form = form.part(field, part);
        }
        self.client
            .put(format!("{}/v1/deployments/{deployment_id}", self.base_url))
            .header("Accept", "application/json")
            .multipart(form)
            .send()
            .await
            .expect("webapi submit package request failed")
    }

    async fn deployment_exists(&self, deployment_id: DeploymentId) -> bool {
        let resp = self
            .client
            .get(format!("{}/v1/deployments/{deployment_id}", self.base_url))
            .header("Accept", "application/json")
            .send()
            .await
            .expect("get deployment request failed");
        match resp.status().as_u16() {
            200 => true,
            404 => false,
            other => panic!("unexpected get deployment status {other}"),
        }
    }

    /// Submit a manifest as a CAS-efficient package: preflight without blobs,
    /// then retry attaching only the files the server reports missing.
    /// Returns the new deployment ID.
    async fn webapi_upload_and_submit_manifest(
        &self,
        deployment_toml_path: &std::path::Path,
    ) -> DeploymentId {
        let prepared =
            crate::config::deployment::prepare_deployment_manifest_from_disk(deployment_toml_path)
                .await
                .expect("cannot prepare deployment manifest");
        let deployment_id = DeploymentId::generate();
        let mut resp = self
            .webapi_submit_package(deployment_id, &prepared.deployment_toml, Vec::new(), false)
            .await;
        if resp.status() == reqwest::StatusCode::CONFLICT {
            let body: Value = resp.json().await.unwrap();
            let missing: Vec<&str> = body["missing_files"]
                .as_array()
                .expect("409 must list missing_files")
                .iter()
                .filter_map(|issue| issue["digest"].as_str())
                .collect();
            let files = prepared
                .files
                .iter()
                .filter(|file| missing.contains(&file.digest.to_string().as_str()))
                .map(|file| {
                    (
                        file.digest.to_string(),
                        file.path.clone(),
                        file.bytes.clone(),
                    )
                })
                .collect();
            resp = self
                .webapi_submit_package(deployment_id, &prepared.deployment_toml, files, false)
                .await;
        }
        assert_eq!(resp.status(), reqwest::StatusCode::NO_CONTENT);
        deployment_id
    }

    /// Hot-redeploy to the given deployment via the Web API.
    async fn webapi_switch_hot_redeploy(&self, deployment_id: DeploymentId) {
        let resp = self
            .client
            .put(format!(
                "{}/v1/deployments/{deployment_id}/switch",
                self.base_url
            ))
            .header("Accept", "application/json")
            .json(&json!({ "apply": true }))
            .send()
            .await
            .expect("webapi switch deployment request failed");
        let body = lifecycle_response(
            resp,
            "/v1/deployments/{deployment_id}/switch",
            "put",
            reqwest::StatusCode::OK,
        )
        .await;
        let body: rest_types::lifecycle::DeploymentSwitchResponse =
            serde_json::from_value(body).unwrap();
        assert_eq!(
            body.ok,
            rest_types::lifecycle::DeploymentSwitchOutcome::Switched
        );
    }

    /// Apply `mutate` to the active deployment's manifest, submit it and hot-redeploy to it.
    #[instrument(skip_all)]
    async fn submit_and_hot_redeploy(&self, mutate: impl FnOnce(&mut DocumentMut)) {
        let deployment_id = self.submit_modified_deployment(mutate).await;
        self.webapi_switch_hot_redeploy(deployment_id).await;
    }
}

#[tokio::test]
async fn api_auth_should_deny_unauthenticated_requests() {
    let hashed_token = "hashed-secret-token";
    let explicit_token = "explicit-secret-token";
    let server = TestServer::start_empty_with_auth(
        test_addr!(89),
        &format!(
            "api.token_hashes = [\"{hash}\"]",
            hash = crate::api::token_hash(hashed_token)
        ),
        Some(explicit_token),
    )
    .await;

    let url = format!("{}/v1/functions", server.base_url);
    for wrong in [
        server.client.get(&url),
        server.client.get(&url).bearer_auth("wrong-token"),
    ] {
        assert_eq!(
            wrong.send().await.unwrap().status(),
            reqwest::StatusCode::UNAUTHORIZED
        );
    }

    let resp = server
        .client
        .get(&url)
        .bearer_auth(hashed_token)
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "status: {}", resp.status());

    let resp = server
        .client
        .get(&url)
        .bearer_auth(explicit_token)
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "status: {}", resp.status());

    server.shutdown().await;
}

/// Regression test for hot-deploying local WASM files to a server that starts empty.
///
/// Starts with an empty deployment (no components, no CAS blobs), then runs the
/// `deployment apply` flow over REST: upload every referenced WASM blob, submit the verbatim
/// manifest, and hot-redeploy. This used to fail because the server resolved the stored
/// manifest against a synthetic `/deployment` root and then tried to read the WASM from that
/// non-existent path instead of materializing the uploaded blobs from the CAS.
#[tokio::test]
async fn deploy_local_wasm_to_empty_server() {
    let server = TestServer::start_empty(test_addr!(78)).await;

    let fixture = crate::command::test_support::target_aware_deployment_fixture_from_text(
        &get_workspace_dir(),
        r#"[[activity_wasm]]
name = "test_programs_fibo_activity"
location = "target/wasm-cache/test_programs_fibo_activity.wasm"
max_retries = 0

[[workflow_wasm]]
name = "test_programs_fibo_workflow"
location = "target/wasm-cache/test_programs_fibo_workflow_component.wasm"

[[webhook_endpoint_wasm]]
name = "test_programs_fibo_webhook"
location = "target/wasm-cache/test_programs_fibo_webhook.wasm"
routes = [{ methods = ["GET"], route = "/fibo/:N/:ITERATIONS" }]
"#,
    )
    .await
    .unwrap();
    let deployment_id = server
        .webapi_upload_and_submit_manifest(fixture.path())
        .await;
    let response = server
        .client
        .put(format!(
            "{}/v1/deployments/{deployment_id}/switch",
            server.base_url
        ))
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    let body = lifecycle_response(
        response,
        "/v1/deployments/{deployment_id}/switch",
        "put",
        reqwest::StatusCode::OK,
    )
    .await;
    assert_eq!(body["ok"], "restart_required");
    server.webapi_switch_hot_redeploy(deployment_id).await;

    // The WASM components from the manifest are now live.
    let components = server.list_components().await;
    let rendered = serde_json::to_string(&components).unwrap();
    for name in [
        "test_programs_fibo_activity",
        "test_programs_fibo_workflow",
        "test_programs_fibo_webhook",
    ] {
        assert!(
            rendered.contains(name),
            "expected component `{name}` after hot-redeploy, got: {rendered}"
        );
    }

    server.shutdown().await;
}

async fn lifecycle_response(
    response: reqwest::Response,
    path: &str,
    method: &str,
    status: reqwest::StatusCode,
) -> Value {
    assert_eq!(response.status(), status);
    let media_type = response.headers()[reqwest::header::CONTENT_TYPE]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned();
    let contract: Value = serde_json::from_str(rest_types::lifecycle::OPENAPI).unwrap();
    let mut schema = contract["paths"][path][method]["responses"][status.as_str()]["content"]
        [&media_type]["schema"]
        .clone();
    assert!(
        schema.is_object(),
        "undocumented response: {method} {path} {status} {media_type}"
    );
    schema["components"] = contract["components"].clone();
    let body = response.text().await.unwrap();
    let body = if media_type == "application/json" {
        serde_json::from_str(&body).unwrap()
    } else {
        Value::String(body)
    };
    jsonschema::options()
        .with_draft(jsonschema::Draft::Draft202012)
        .should_validate_formats(true)
        .build(&schema)
        .unwrap()
        .validate(&body)
        .unwrap();
    body
}

#[derive(Debug)]
struct AdvanceExecutionSummary {
    execution_id: String,
    steps: usize,
    retval: serde_json::Value,
}

impl TestServer {
    async fn submit_paused_webapi(
        &self,
        execution_id: &str,
        ffqn: &str,
        params: Vec<Value>,
    ) -> reqwest::Response {
        self.client
            .put(format!("{}/v1/executions/{execution_id}", self.base_url))
            .header("Accept", "application/json")
            .json(&json!({
                "ffqn": ffqn,
                "params": params,
                "paused": true,
            }))
            .send()
            .await
            .expect("submit paused request failed")
    }

    /// Seed a cancellable parent workflow blocked on a join set holding an unfinished
    /// **uncancellable** child, directly in the server's sqlite DB, and return the
    /// parent id.
    ///
    /// The uncancellable child is a permanent await barrier, so once the parent is
    /// cancelled the cancellation driver can never advance it to `Finished` — it stays
    /// `Cancelling` for the rest of the test. Cancelling a trivial workflow instead
    /// races the driver, which finishes an empty-join-set cancellation on its next tick.
    async fn seed_cancellable_parent_blocked_on_uncancellable_child(
        &self,
    ) -> concepts::ExecutionId {
        use concepts::prefixed_ulid::DEPLOYMENT_ID_DUMMY;
        use concepts::storage::{
            AppendRequest, CreateRequest, ExecutionRequest, HistoryEvent, JoinSetRequest,
        };
        use concepts::{
            ComponentId, ExecutionId, ExecutionMetadata, JoinSetId, JoinSetKind, Params, StrVariant,
        };

        const PARENT_FFQN: FunctionFqn =
            FunctionFqn::new_static("testing:cancel/ifc", "parent-cancellable");
        const CHILD_FFQN: FunctionFqn = FunctionFqn::new_static("testing:cancel/ifc", "child");

        let pool = SqlitePool::new(&self.sqlite_file, SqliteConfig::default())
            .await
            .unwrap();
        let conn = pool.connection().await.unwrap();
        let now = chrono::Utc::now();
        let create = |execution_id: ExecutionId, ffqn: FunctionFqn| CreateRequest {
            created_at: now,
            execution_id,
            ffqn,
            params: Params::empty(),
            parent: None,
            scheduled_at: now,
            component_id: ComponentId::dummy_workflow(),
            deployment_id: DEPLOYMENT_ID_DUMMY,
            metadata: ExecutionMetadata::empty(),
            scheduled_by: None,
            paused: false,
            max_persisted_value_size_bytes: u64::MAX,
        };

        let parent_id = ExecutionId::generate();
        let version = conn
            .create(create(parent_id.clone(), PARENT_FFQN))
            .await
            .unwrap();
        let join_set_id = JoinSetId::new(JoinSetKind::OneOff, StrVariant::empty()).unwrap();
        let version = conn
            .append(
                parent_id.clone(),
                version,
                AppendRequest {
                    created_at: now,
                    event: ExecutionRequest::HistoryEvent {
                        event: HistoryEvent::JoinSetCreate {
                            join_set_id: join_set_id.clone(),
                        },
                    },
                },
            )
            .await
            .unwrap();
        let child_id = parent_id.next_level(&join_set_id);
        conn.append(
            parent_id.clone(),
            version,
            AppendRequest {
                created_at: now,
                event: ExecutionRequest::HistoryEvent {
                    event: HistoryEvent::JoinSetRequest {
                        join_set_id,
                        request: JoinSetRequest::ChildExecutionRequest {
                            child_execution_id: child_id.clone(),
                            target_ffqn: CHILD_FFQN,
                            params: concepts::storage::PersistedParams::Inline(Params::empty()),
                            params_hash: None,
                            result: Ok(()),
                        },
                    },
                },
            },
        )
        .await
        .unwrap();
        conn.create(create(ExecutionId::Derived(child_id), CHILD_FFQN))
            .await
            .unwrap();
        pool.close().await;
        parent_id
    }

    /// Cancel an execution out-of-band, retrying until it exists and the
    /// request is accepted (a parent submits its child lazily while running).
    async fn cancel_execution_with_retries(&self, execution_id: &str) {
        loop {
            let resp = self
                .client
                .put(format!(
                    "{}/v1/executions/{execution_id}/cancel",
                    self.base_url
                ))
                .header("Accept", "application/json")
                .send()
                .await
                .expect("cancel execution request failed");
            if resp.status().is_success() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Cancel a delay out-of-band, retrying until the workflow has submitted it.
    async fn cancel_delay_with_retries(&self, delay_id: &str) {
        loop {
            let resp = self
                .client
                .put(format!("{}/v1/delays/{delay_id}/cancel", self.base_url))
                .header("Accept", "application/json")
                .send()
                .await
                .expect("cancel delay request failed");
            if resp.status().is_success() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn step_execution_until_finished_webapi(
        &self,
        ffqn: &str,
        params: Vec<Value>,
    ) -> AdvanceExecutionSummary {
        #[derive(Debug, Deserialize)]
        #[serde(tag = "type", rename_all = "snake_case")]
        pub(crate) enum AdvanceResponseDeser {
            Finished {
                value: serde_json::Value, // RetVal -> Value for deserialization
            },
            InProgress,
        }

        let exec_id = self.generate_execution_id().await;
        let submit = self.submit_paused_webapi(&exec_id, ffqn, params).await;
        assert_eq!(submit.status().as_u16(), 201);

        let mut steps = 0;
        loop {
            let replay = self.replay(&exec_id).await;
            let replay_status = replay.status().as_u16();
            assert!(
                replay_status == 200 || replay_status == 409,
                "unexpected replay status: {replay_status}"
            );
            let replay_body: ReplayResponseSer = replay.json().await.unwrap();
            if ffqn == "testing:integration/workflow-call-stub.call-stub" {
                assert_eq!(
                    u64::try_from(steps * 2).unwrap(),
                    replay_body.replayed_event_count
                );
                assert_eq!(
                    u32::try_from(steps * 2 + 1).unwrap(),
                    replay_body.replay_version
                );
            }
            let captured_writes = match replay_body.outcome {
                ReplayOutcomeSer::Advanceable { captured_writes }
                | ReplayOutcomeSer::ReplayFailed {
                    captured_writes, ..
                } => captured_writes,
                ReplayOutcomeSer::Finished { retval: _ } => {
                    panic!("should have been returned as `advance` response first");
                }
                ReplayOutcomeSer::Blocked => {
                    unreachable!("blocked state is not created by any test")
                }
            };
            assert!(!captured_writes.is_empty());
            steps += 1;
            let advance = self
                .client
                .put(format!("{}/v1/executions/{exec_id}/advance", self.base_url))
                .header("Accept", "application/json")
                .json(&json!({
                    "captured_writes": captured_writes,
                    "persist_backtrace": true,
                }))
                .send()
                .await
                .expect("advance request failed");
            assert_eq!(
                advance.status().as_u16(),
                200,
                "advance failed: {}",
                advance.text().await.unwrap()
            );
            let advance_body: AdvanceResponseDeser = advance.json().await.unwrap();
            match advance_body {
                AdvanceResponseDeser::Finished { value: retval } => {
                    return AdvanceExecutionSummary {
                        execution_id: exec_id,
                        steps,
                        retval,
                    };
                }
                AdvanceResponseDeser::InProgress => {
                    // continue the loop
                }
            }
        }
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

#[rstest::rstest]
#[tokio::test]
async fn js_import_policy_component_metadata(
    #[values(JsRuntime::BoaWasm, JsRuntime::V8)] runtime: JsRuntime,
) {
    let deployment = r#"
[[workflow_js]]
name = "target"
ffqn = "app:target/api.run"
content = "export default () => 'ok';"
params = []
return_type = "result<string, string>"

[[workflow_js]]
name = "typed_workflow"
ffqn = "app:caller/api.typed"
content = "import {} from 'app:target/api'; export default () => 'ok';"
params = []
return_type = "result<string, string>"

[[workflow_js]]
name = "dynamic_workflow"
ffqn = "app:caller/api.dynamic"
location = "workflow.js"
params = []
return_type = "result<string, string>"

[[workflow_js]]
name = "mixed_workflow"
ffqn = "app:caller/api.mixed"
content = "import 'app:target/api'; import 'obelisk:workflow-dynamic@1.0.0'; export default () => 'ok';"
params = []
return_type = "result<string, string>"

[[webhook_endpoint_js]]
name = "typed_webhook"
content = "export {run as target} from 'app:target/api'; export default () => new Response('ok');"
routes = [{ methods = ["GET"], route = "/typed" }]

[[webhook_endpoint_js]]
name = "dynamic_webhook"
location = "webhook.js"
routes = [{ methods = ["GET"], route = "/dynamic" }]

[[webhook_endpoint_js]]
name = "mixed_webhook"
content = "import 'app:target/api'; export * from 'obelisk:webhook-dynamic@1.0.0'; export default () => new Response('ok');"
routes = [{ methods = ["GET"], route = "/mixed" }]
"#;
    let server = TestServer::start_inline_deployment_with_js_runtime(
        match runtime {
            JsRuntime::BoaWasm => test_addr!(40_156),
            JsRuntime::V8 => test_addr!(40_157),
        },
        "",
        deployment,
        &[
            (
                "workflow.js",
                "import './workflow-helper.js'; export default () => 'ok';",
            ),
            (
                "workflow-helper.js",
                "export {submit} from 'obelisk:workflow-dynamic@1.0.0';",
            ),
            (
                "webhook.js",
                "import './webhook-helper.js'; export default () => new Response('ok');",
            ),
            (
                "webhook-helper.js",
                "import {} from 'obelisk:webhook-dynamic@1.0.0';",
            ),
        ],
        runtime.mode(),
    )
    .await;
    let response = server
        .client
        .get(format!("{}/v1/components?imports=true", server.base_url))
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
    let components = response.json::<Value>().await.unwrap();
    for (name, typed, dynamic) in [
        ("target", false, None),
        ("typed_workflow", true, None),
        ("dynamic_workflow", false, Some("workflow")),
        ("mixed_workflow", true, Some("workflow")),
        ("typed_webhook", true, None),
        ("dynamic_webhook", false, Some("webhook")),
        ("mixed_webhook", true, Some("webhook")),
    ] {
        let component = components
            .as_array()
            .unwrap()
            .iter()
            .find(|component| component["component_id"]["name"] == name)
            .unwrap();
        let imports = component["imports"].as_array().unwrap();
        assert_eq!(
            imports
                .iter()
                .any(|import| import["ffqn"] == "app:target/api.run"),
            typed,
            "{name}: {component}"
        );
        assert_eq!(
            imports.iter().any(|import| import["ffqn"]
                .as_str()
                .unwrap()
                .contains("-dynamic-support@")),
            dynamic.is_some(),
            "{name}: {component}"
        );
        if let Some(kind) = dynamic {
            assert!(
                imports.iter().any(|import| import["ffqn"]
                    .as_str()
                    .unwrap()
                    .starts_with(&format!("obelisk:{kind}/{kind}-dynamic-support@"))),
                "{name}: {component}"
            );
        }
    }
    server.shutdown().await;
}

#[tokio::test]
async fn list_components_webapi_by_explicit_deployment_id() {
    let server = TestServer::start(test_addr!(40_100)).await;
    const NEW_STUB_NAME: &str = "explicit_deployment_stub";

    let second_deployment_id = server
        .submit_modified_deployment(|doc| {
            append_inline_stub(
                doc,
                NEW_STUB_NAME,
                "testing:integration/stubs.explicit-deployment-stub",
            );
        })
        .await;

    let current_components = server.list_components().await;
    assert!(
        !current_components
            .as_array()
            .unwrap()
            .iter()
            .any(|component| component["component_id"]["name"] == NEW_STUB_NAME),
        "inactive deployment component must not appear without explicit deployment_id"
    );

    let explicit_components = server
        .list_components_for_deployment(Some(second_deployment_id))
        .await;
    assert!(
        explicit_components
            .as_array()
            .unwrap()
            .iter()
            .any(|component| component["component_id"]["name"] == NEW_STUB_NAME),
        "explicit deployment_id must return the submitted deployment's components"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn list_components_webapi_filters_with_explicit_deployment_id() {
    let server = TestServer::start(test_addr!(40_101)).await;
    const NEW_STUB_NAME: &str = "explicit_deployment_stub_filters";

    let second_deployment_id = server
        .submit_modified_deployment(|doc| {
            append_inline_stub(
                doc,
                NEW_STUB_NAME,
                "testing:integration/stubs.explicit-deployment-filters",
            );
        })
        .await;

    let explicit_components = server
        .list_components_for_deployment(Some(second_deployment_id))
        .await;
    let target = explicit_components
        .as_array()
        .unwrap()
        .iter()
        .find(|component| component["component_id"]["name"] == NEW_STUB_NAME)
        .unwrap();
    let digest = target["component_id"]["component_digest"].as_str().unwrap();

    let filtered: Value = server
        .client
        .get(format!(
            "{}/v1/components?deployment_id={}&name={}&type=activity_stub&digest={}&exports=true&submittable=false",
            server.base_url, second_deployment_id, NEW_STUB_NAME, digest
        ))
        .header("Accept", "application/json")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let filtered = filtered.as_array().unwrap();
    assert_eq!(1, filtered.len());
    assert_eq!(NEW_STUB_NAME, filtered[0]["component_id"]["name"]);
    assert_eq!(
        "activity_stub",
        filtered[0]["component_id"]["component_type"]
    );
    assert_eq!(digest, filtered[0]["component_id"]["component_digest"]);
    assert_eq!(1, filtered[0]["exports"].as_array().unwrap().len());

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

#[tokio::test]
async fn app_policy_webapi_reports_running_policy() {
    let server = TestServer::start(test_addr!(40_144)).await;
    let app: crate::config::app::AppConfigToml =
        toml::from_str(&std::fs::read_to_string(server.tmp_dir.path().join("app.toml")).unwrap())
            .unwrap();
    let expected_digest = app.policy().digest().to_string();
    let response = server
        .client
        .get(format!("{}/v1/info", server.base_url))
        .send()
        .await
        .unwrap();
    let info = lifecycle_response(response, "/v1/info", "get", reqwest::StatusCode::OK).await;
    let _: rest_types::lifecycle::ServerInfoResponse =
        serde_json::from_value(info.clone()).unwrap();
    assert_eq!(info["app_config_digest"], expected_digest);

    for endpoint in ["deployment-id", "execution-id"] {
        let response = server
            .client
            .get(format!("{}/v1/{endpoint}", server.base_url))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        assert_eq!(
            response.headers()["content-type"],
            "text/plain; charset=utf-8"
        );
        let id = response.text().await.unwrap();
        if endpoint == "deployment-id" {
            assert_eq!(info["deployment_id"], id);
        } else {
            id.parse::<concepts::ExecutionId>().unwrap();
        }
    }

    let policy = server
        .client
        .get(format!("{}/v1/app-policy", server.base_url))
        .send()
        .await
        .unwrap();
    let policy = lifecycle_response(policy, "/v1/app-policy", "get", reqwest::StatusCode::OK).await;
    let _: rest_types::lifecycle::AppPolicyV1 = serde_json::from_value(policy.clone()).unwrap();
    assert_eq!(policy, serde_json::to_value(app.policy()).unwrap());

    let policy = server
        .client
        .get(format!("{}/v1/app-policy", server.base_url))
        .header("Accept", "application/toml")
        .send()
        .await
        .unwrap();
    assert_eq!(policy.status(), reqwest::StatusCode::OK);
    assert_eq!(
        policy.headers()[reqwest::header::CONTENT_TYPE],
        "application/toml"
    );
    let policy = lifecycle_response(policy, "/v1/app-policy", "get", reqwest::StatusCode::OK).await;
    let policy: toml::Value = toml::from_str(policy.as_str().unwrap()).unwrap();
    assert_eq!(policy, toml::Value::try_from(app.policy()).unwrap());

    let switch_path = "/v1/deployments/{deployment_id}/switch";
    let switch_url = format!(
        "{}/v1/deployments/{}/switch",
        server.base_url,
        info["deployment_id"].as_str().unwrap()
    );
    for payload in [json!({"apply": null}), json!({"apply": "true"})] {
        let response = server
            .client
            .put(&switch_url)
            .json(&payload)
            .send()
            .await
            .unwrap();
        lifecycle_response(
            response,
            switch_path,
            "put",
            reqwest::StatusCode::UNPROCESSABLE_ENTITY,
        )
        .await;
    }
    let response = server
        .client
        .put(&switch_url)
        .body("{}")
        .send()
        .await
        .unwrap();
    lifecycle_response(
        response,
        switch_path,
        "put",
        reqwest::StatusCode::UNSUPPORTED_MEDIA_TYPE,
    )
    .await;
    for accept in ["application/json", "*/*"] {
        let response = server
            .client
            .put(&switch_url)
            .header("Accept", accept)
            .json(&json!({"apply": true, "allow_unavailable_runtime_config": true}))
            .send()
            .await
            .unwrap();
        lifecycle_response(
            response,
            switch_path,
            "put",
            reqwest::StatusCode::BAD_REQUEST,
        )
        .await;
        let response = server
            .client
            .put(format!(
                "{}/v1/deployments/{}/switch",
                server.base_url,
                DeploymentId::generate()
            ))
            .header("Accept", accept)
            .json(&json!({}))
            .send()
            .await
            .unwrap();
        lifecycle_response(response, switch_path, "put", reqwest::StatusCode::NOT_FOUND).await;
    }
    let response = server
        .client
        .put(&switch_url)
        .header("Accept", "text/plain")
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    lifecycle_response(
        response,
        switch_path,
        "put",
        reqwest::StatusCode::NOT_ACCEPTABLE,
    )
    .await;
    let response = server
        .client
        .get(format!("{}/v1/app-policy", server.base_url))
        .header("Accept", "text/plain")
        .send()
        .await
        .unwrap();
    lifecycle_response(
        response,
        "/v1/app-policy",
        "get",
        reqwest::StatusCode::NOT_ACCEPTABLE,
    )
    .await;
    let published: Value = server
        .client
        .get(format!("{}/openapi.json", server.base_url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let contract: Value = serde_json::from_str(rest_types::lifecycle::OPENAPI).unwrap();
    for (path, definition) in contract["paths"].as_object().unwrap() {
        assert_eq!(&published["paths"][path], definition);
    }
    for (name, schema) in contract["components"]["schemas"].as_object().unwrap() {
        assert_eq!(&published["components"]["schemas"][name], schema);
    }

    let deployments: Value = server
        .client
        .get(format!("{}/v1/deployments", server.base_url))
        .header("Accept", "application/json")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let active = deployments
        .as_array()
        .unwrap()
        .iter()
        .find(|d| d["status"] == "active")
        .unwrap();
    assert_eq!(active["last_active_app_config_digest"], expected_digest);
    let deployment: Value = server
        .client
        .get(format!(
            "{}/v1/deployments/{}",
            server.base_url,
            active["deployment_id"].as_str().unwrap()
        ))
        .header("Accept", "application/json")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(deployment["last_active_app_config_digest"], expected_digest);

    let db_pool = SqlitePool::new(&server.sqlite_file, SqliteConfig::default())
        .await
        .unwrap();
    let mut event = concepts::storage::SystemEvent::new(
        concepts::storage::SystemEventCode::MaintenanceGcFailed,
        None,
        None,
        json!({"reason": "test"}),
    )
    .unwrap();
    event.app_config_digest = Some(expected_digest.clone());
    let event_id = event.event_id;
    db_pool
        .admin_conn()
        .await
        .unwrap()
        .append_system_event(event)
        .await
        .unwrap();
    db_pool.close().await;
    let event: Value = server
        .client
        .get(format!(
            "{}/v1/admin/system-events/{event_id}",
            server.base_url
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(event["app_config_digest"], expected_digest);

    server.shutdown().await;
}

#[tokio::test]
async fn submit_deployment_with_generated_id_webapi() {
    let server = TestServer::start(test_addr!(40_105)).await;

    let deployment_toml = server.active_deployment_toml().await;
    let deployment_id = server.webapi_submit_deployment(&deployment_toml).await;

    let resp = server
        .client
        .get(format!(
            "{}/v1/deployments/{deployment_id}",
            server.base_url
        ))
        .header("Accept", "application/json")
        .send()
        .await
        .expect("get generated deployment failed");
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    server.shutdown().await;
}

#[tokio::test]
async fn submit_deployment_is_idempotent_by_id_and_digest_webapi() {
    let server = TestServer::start(test_addr!(40_104)).await;

    // Read the active deployment's verbatim manifest so we can resubmit it.
    let deployment_toml = server.active_deployment_toml().await;

    let deployment_id = DeploymentId::generate();

    // First submission under the explicit ID creates the deployment.
    let resp1 = server
        .webapi_submit_deployment_with_id(&deployment_toml, deployment_id)
        .await;
    assert!(
        resp1.status().is_success(),
        "first submit failed: {}",
        resp1.status()
    );
    assert_eq!(resp1.status(), reqwest::StatusCode::NO_CONTENT);

    // Resubmitting the identical manifest under the same ID is an idempotent no-op.
    let resp2 = server
        .webapi_submit_deployment_with_id(&deployment_toml, deployment_id)
        .await;
    assert!(
        resp2.status().is_success(),
        "idempotent resubmit failed: {}",
        resp2.status()
    );
    assert_eq!(resp2.status(), reqwest::StatusCode::NO_CONTENT);

    // Submitting a different manifest under the same ID is rejected as a digest conflict.
    let mut mutated = deployment_toml.parse::<DocumentMut>().unwrap();
    append_inline_stub(
        &mut mutated,
        "idempotency_conflict_stub",
        "testing:integration/stubs.idempotency-conflict",
    );
    let mutated_toml = mutated.to_string();
    let resp3 = server
        .webapi_submit_deployment_with_id(&mutated_toml, deployment_id)
        .await;
    assert_eq!(
        resp3.status(),
        reqwest::StatusCode::CONFLICT,
        "different config under same ID must be rejected as a digest conflict"
    );

    server.shutdown().await;
}

/// A deployment whose JS imports an interface that no component exports fails to
/// compile+link on the server. This is a structural error that no runtime config can
/// resolve, so it must be rejected regardless of `allow_unavailable_runtime_config` and persist
/// nothing. The manifest is submitted verbatim over REST (inline content, no attached
/// files) so verification does not lean on any client-side import checking.
#[tokio::test]
async fn submit_broken_js_import_fails_regardless_of_runtime_config_check_webapi() {
    let server = TestServer::start(test_addr!(92)).await;

    // Inline webhook importing an interface no component exports. Inline content needs
    // no attached file blobs, so the whole package travels in the manifest.
    let deployment_toml = r#"
[[webhook_endpoint_js]]
name = "broken_import"
content = """
import { add } from 'testing:integration/nonexistent';
export default function handle(request) {
    return Response.json({ result: add(5, 7) });
}
"""
routes = [{ methods = ["GET"], route = "/broken-import" }]
"#;

    // Both a strict and an allow-unavailable submit must be rejected: the missing import
    // is a link failure, not a missing env var / secret / exec allowlist entry, so no
    // runtime config tolerance can make it verify.
    for allow_unavailable in [false, true] {
        let deployment_id = DeploymentId::generate();
        let resp = server
            .webapi_submit_package(
                deployment_id,
                deployment_toml,
                Vec::new(),
                allow_unavailable,
            )
            .await;
        let status = resp.status();
        let body = resp.text().await.unwrap();
        assert!(
            !status.is_success(),
            "allow_unavailable={allow_unavailable} submit of a broken import must fail"
        );
        assert!(
            body.contains("testing:integration/nonexistent"),
            "allow_unavailable={allow_unavailable} error must name the unresolved import, got: {body}"
        );
        assert!(
            !server.deployment_exists(deployment_id).await,
            "a broken deployment must not be persisted"
        );
    }

    server.shutdown().await;
}

/// A webhook whose entry JS imports a relative module that is not part of the submitted
/// package must be rejected server-side. This mirrors a broken client (e.g. the
/// workflow-agent) that declares/uploads only the entry file: the manifest carries the
/// entry's `content_digest` but no `component_files`, so the server must walk the import
/// graph from the CAS and refuse the open graph rather than persisting a webhook that
/// only traps at request time. Like the missing-interface case, no runtime config can
/// resolve it, so both strict and `allow_unavailable_runtime_config` submits must fail and persist nothing.
#[tokio::test]
async fn submit_webhook_missing_js_import_file_is_rejected_webapi() {
    let server = TestServer::start(test_addr!(93)).await;

    // A valid two-file graph on disk, so `prepare` computes the entry digest for us.
    let dir = tempfile::tempdir().unwrap();
    tokio::fs::create_dir_all(dir.path().join("webhook/lib"))
        .await
        .unwrap();
    tokio::fs::write(
        dir.path().join("webhook/ui-api.js"),
        "import { util } from './lib/util.js';\n\
         export default function handle(request) { return new Response(util()); }\n",
    )
    .await
    .unwrap();
    tokio::fs::write(
        dir.path().join("webhook/lib/util.js"),
        "export function util() { return 'ok'; }\n",
    )
    .await
    .unwrap();
    let deployment_toml_path = dir.path().join("deployment.toml");
    tokio::fs::write(
        &deployment_toml_path,
        r#"
[[webhook_endpoint_js]]
name = "broken_import"
location = "webhook/ui-api.js"
routes = [{ methods = ["GET"], route = "/" }]
"#,
    )
    .await
    .unwrap();
    let prepared =
        crate::config::deployment::prepare_deployment_manifest_from_disk(&deployment_toml_path)
            .await
            .unwrap();

    // Simulate the broken client: drop `component_files` so only the entry is declared,
    // and attach only the entry blob. The manifest still carries the entry's content_digest.
    let mut doc = prepared.deployment_toml.parse::<DocumentMut>().unwrap();
    if let Some(tables) = doc
        .get_mut("webhook_endpoint_js")
        .and_then(toml_edit::Item::as_array_of_tables_mut)
    {
        for table in tables.iter_mut() {
            table.remove("component_files");
        }
    }
    let broken_toml = doc.to_string();
    let entry = prepared
        .files
        .iter()
        .find(|file| file.path.ends_with("ui-api.js"))
        .expect("prepared package contains the entry file");

    for allow_unavailable in [false, true] {
        let deployment_id = DeploymentId::generate();
        let resp = server
            .webapi_submit_package(
                deployment_id,
                &broken_toml,
                vec![(
                    entry.digest.to_string(),
                    entry.path.clone(),
                    entry.bytes.clone(),
                )],
                allow_unavailable,
            )
            .await;
        let status = resp.status();
        let body = resp.text().await.unwrap();
        assert!(
            !status.is_success(),
            "allow_unavailable={allow_unavailable} submit of an open JS graph must fail"
        );
        assert!(
            body.contains("webhook/lib/util.js"),
            "allow_unavailable={allow_unavailable} error must name the missing module, got: {body}"
        );
        assert!(
            !server.deployment_exists(deployment_id).await,
            "a webhook with an unresolved import must not be persisted"
        );
    }

    server.shutdown().await;
}

/// A manifest that declares a `component_files` entry its module graph never imports is
/// rejected at submit. Otherwise the stray blob is persisted and the deployment -> digest
/// mapping the orphan GC consults would pin a dead file forever. Built by preparing a
/// valid graph, then hand-adding an unreferenced module + its blob (a broken client).
#[tokio::test]
async fn submit_rejects_stray_declared_component_file_webapi() {
    let server = TestServer::start(test_addr!(94)).await;

    let dir = tempfile::tempdir().unwrap();
    tokio::fs::create_dir_all(dir.path().join("webhook"))
        .await
        .unwrap();
    tokio::fs::write(
        dir.path().join("webhook/ui-api.js"),
        "import { util } from './util.js';\n\
         export default function handle(request) { return new Response(util()); }\n",
    )
    .await
    .unwrap();
    tokio::fs::write(
        dir.path().join("webhook/util.js"),
        "export function util() { return 'ok'; }\n",
    )
    .await
    .unwrap();
    let deployment_toml_path = dir.path().join("deployment.toml");
    tokio::fs::write(
        &deployment_toml_path,
        r#"
[[webhook_endpoint_js]]
name = "stray"
location = "webhook/ui-api.js"
routes = [{ methods = ["GET"], route = "/" }]
"#,
    )
    .await
    .unwrap();
    let prepared =
        crate::config::deployment::prepare_deployment_manifest_from_disk(&deployment_toml_path)
            .await
            .unwrap();

    // Hand-add an extra module nothing imports, plus its blob: a broken client declaring
    // more than the graph needs.
    let stray_source = "export const unused = 1;\n";
    let stray_hash: [u8; 32] = <Sha256 as sha2::Digest>::digest(stray_source.as_bytes()).into();
    let stray_digest =
        concepts::ContentDigest(concepts::component_id::Digest(stray_hash)).to_string();
    let mut doc = prepared.deployment_toml.parse::<DocumentMut>().unwrap();
    let tables = doc
        .get_mut("webhook_endpoint_js")
        .and_then(toml_edit::Item::as_array_of_tables_mut)
        .expect("webhook section");
    for table in tables.iter_mut() {
        let component_files = table
            .get_mut("component_files")
            .and_then(toml_edit::Item::as_table_like_mut)
            .expect("prepare generated component_files for a multi-file component");
        component_files.insert("webhook/unused.js", toml_edit::value(stray_digest.clone()));
    }
    let broken_toml = doc.to_string();

    let mut files: Vec<(String, String, Vec<u8>)> = prepared
        .files
        .iter()
        .map(|file| {
            (
                file.digest.to_string(),
                file.path.clone(),
                file.bytes.clone(),
            )
        })
        .collect();
    files.push((
        stray_digest,
        "webhook/unused.js".to_string(),
        stray_source.as_bytes().to_vec(),
    ));

    let deployment_id = DeploymentId::generate();
    let resp = server
        .webapi_submit_package(deployment_id, &broken_toml, files, false)
        .await;
    let status = resp.status();
    let body = resp.text().await.unwrap();
    assert!(
        !status.is_success(),
        "a stray declared component file must be rejected"
    );
    assert!(
        body.contains("webhook/unused.js"),
        "error must name the stray file, got: {body}"
    );
    assert!(
        !server.deployment_exists(deployment_id).await,
        "a deployment with a stray declared file must not be persisted"
    );

    server.shutdown().await;
}

/// Submit writes referenced blobs to the CAS before validating (persist-then-validate). A
/// submit rejected after that write must reclaim its blobs, or they leak as orphans until a
/// manual GC. Here a webhook file blob is written, then compile+link fails on a missing
/// interface; the follow-up GC must find nothing left to delete.
#[tokio::test]
async fn rejected_submit_reclaims_orphan_blobs_webapi() {
    let server = TestServer::start(test_addr!(95)).await;

    // A single-file webhook (its only import is a WIT interface, so no `component_files`)
    // whose interface no component exports: resolution succeeds and writes the blob, then
    // compile+link fails.
    let dir = tempfile::tempdir().unwrap();
    tokio::fs::write(
        dir.path().join("webhook.js"),
        "import { nope } from 'testing:integration/nonexistent';\n\
         export default function handle(request) { return new Response(nope()); }\n",
    )
    .await
    .unwrap();
    let deployment_toml_path = dir.path().join("deployment.toml");
    tokio::fs::write(
        &deployment_toml_path,
        r#"
[[webhook_endpoint_js]]
name = "orphan_probe"
location = "webhook.js"
routes = [{ methods = ["GET"], route = "/" }]
"#,
    )
    .await
    .unwrap();
    let prepared =
        crate::config::deployment::prepare_deployment_manifest_from_disk(&deployment_toml_path)
            .await
            .unwrap();
    assert_eq!(prepared.files.len(), 1, "webhook must contribute one blob");
    let files = prepared
        .files
        .iter()
        .map(|file| {
            (
                file.digest.to_string(),
                file.path.clone(),
                file.bytes.clone(),
            )
        })
        .collect();
    let resp = server
        .webapi_submit_package(
            DeploymentId::generate(),
            &prepared.deployment_toml,
            files,
            false,
        )
        .await;
    assert!(
        !resp.status().is_success(),
        "compile+link must reject the missing interface"
    );

    let pool = SqlitePool::new(&server.sqlite_file, SqliteConfig::default())
        .await
        .unwrap();
    assert!(
        !pool
            .cas_conn()
            .await
            .unwrap()
            .contains_blob(&prepared.files[0].digest)
            .await
            .unwrap(),
        "a rejected submit must not leak orphan blobs"
    );
    pool.close().await;

    server.shutdown().await;
}

/// Phase 5: `allow_unavailable_runtime_config` governs whether missing env vars / secrets
/// fail verification. Submit is strict by default and tolerant when it is set; a hot
/// redeploy is always strict and rejects it outright.
#[tokio::test]
async fn submit_runtime_config_check_webapi() {
    let server = TestServer::start(test_addr!(79)).await;

    // A JS activity referencing a bare-name env var guaranteed absent from the server
    // environment. Inline content needs no attached file blobs.
    let deployment_toml = r#"
[[activity_js]]
name = "needs_env"
content = "export function run() { return 'ok'; }"
ffqn = "testing:integration/needs-env.run"
env_vars = ["OBELISK_PHASE5_DEFINITELY_MISSING_VAR"]
"#;

    let deployment_id = DeploymentId::generate();
    let switch = |allow_unavailable: bool| {
        server
            .client
            .put(format!(
                "{}/v1/deployments/{deployment_id}/switch",
                server.base_url
            ))
            .header("Accept", "application/json")
            .json(&json!({
                "apply": true,
                "allow_unavailable_runtime_config": allow_unavailable,
            }))
            .send()
    };

    // Strict submit fails on the missing env var and stores nothing.
    let resp = server
        .webapi_submit_package(deployment_id, deployment_toml, Vec::new(), false)
        .await;
    let status = resp.status();
    let body = resp.text().await.unwrap();
    assert!(
        !status.is_success(),
        "strict submit must reject a missing env var"
    );
    assert!(
        body.contains("OBELISK_PHASE5_DEFINITELY_MISSING_VAR"),
        "error must name the missing env var, got: {body}"
    );

    // Allow-unavailable submit tolerates the missing env var and persists.
    let resp = server
        .webapi_submit_package(deployment_id, deployment_toml, Vec::new(), true)
        .await;
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::NO_CONTENT,
        "allow-missing submit must persist"
    );

    // A hot redeploy with allow-unavailable is rejected outright.
    let resp = switch(true).await.unwrap();
    assert!(!resp.status().is_success());
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("`allow_unavailable_runtime_config = true` not allowed with `apply = true`"),
        "unexpected error: {body}"
    );

    // A strict hot redeploy of the same deployment fails because the env var is missing.
    let resp = switch(false).await.unwrap();
    assert!(!resp.status().is_success());
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("OBELISK_PHASE5_DEFINITELY_MISSING_VAR"),
        "error must name the missing env var, got: {body}"
    );

    server.shutdown().await;
}

/// Phase 6: REST deployment submission accepts a `multipart/form-data` package
/// and reports an incomplete package as a structured `409` so the client retries with
/// the missing blobs attached.
#[tokio::test]
async fn submit_deployment_multipart_package_webapi() {
    let server = TestServer::start(test_addr!(80)).await;

    let dir = tempfile::tempdir().unwrap();
    tokio::fs::write(
        dir.path().join("pkg.js"),
        "export function run() { return 'ok'; }",
    )
    .await
    .unwrap();
    let deployment_toml_path = dir.path().join("deployment.toml");
    tokio::fs::write(
        &deployment_toml_path,
        r#"
[[activity_js]]
name = "pkg"
location = "pkg.js"
ffqn = "testing:integration/pkg.run"
"#,
    )
    .await
    .unwrap();
    let prepared =
        crate::config::deployment::prepare_deployment_manifest_from_disk(&deployment_toml_path)
            .await
            .unwrap();
    let toml = prepared.deployment_toml.clone();
    let expected_digest = prepared.files[0].digest.to_string();
    let content = prepared.files[0].bytes.clone();
    let deployment_id = DeploymentId::generate();
    let url = format!("{}/v1/deployments/{deployment_id}", server.base_url);

    // Preflight with no file parts: incomplete package, stored nothing, lists the missing file.
    let resp = server
        .client
        .put(&url)
        .header("Accept", "application/json")
        .multipart(reqwest::multipart::Form::new().text("deployment_toml", toml.clone()))
        .send()
        .await
        .expect("multipart preflight failed");
    assert_eq!(
        resp.status().as_u16(),
        409,
        "incomplete package must be 409"
    );
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["missing_files"].as_array().unwrap().len(), 1);
    assert_eq!(body["missing_files"][0]["digest"], json!(expected_digest));
    assert_eq!(body["missing_files"][0]["section"], json!("activity_js"));
    assert!(
        !server.deployment_exists(deployment_id).await,
        "failed preflight must not persist a deployment"
    );

    // An attached blob not referenced by the manifest is rejected as unexpected.
    let resp = server
        .webapi_submit_package(
            deployment_id,
            &toml,
            vec![(
                "file".to_string(),
                "pkg.js".to_string(),
                b"unexpected".to_vec(),
            )],
            false,
        )
        .await;
    assert_eq!(resp.status().as_u16(), 409, "unexpected blob must be 409");
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["unexpected_files"].as_array().unwrap().len(), 1);

    // A blob whose supplied digest disagrees with its bytes is reported as a mismatch.
    let mismatch_part = reqwest::multipart::Part::bytes(content.clone())
        .file_name("pkg.js")
        .mime_str("application/octet-stream")
        .unwrap();
    let resp = server
        .client
        .put(&url)
        .header("Accept", "application/json")
        .multipart(
            reqwest::multipart::Form::new()
                .text("deployment_toml", toml.clone())
                .part(
                    "sha256:0000000000000000000000000000000000000000000000000000000000000000",
                    mismatch_part,
                ),
        )
        .send()
        .await
        .expect("multipart mismatch submit failed");
    assert_eq!(resp.status().as_u16(), 409, "digest mismatch must be 409");
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["digest_mismatches"].as_array().unwrap().len(), 1);

    // Retry with the correct blob (field name carries the digest): the package is complete.
    let part = reqwest::multipart::Part::bytes(content)
        .file_name("pkg.js")
        .mime_str("application/octet-stream")
        .unwrap();
    let resp = server
        .client
        .put(&url)
        .header("Accept", "application/json")
        .multipart(
            reqwest::multipart::Form::new()
                .text("deployment_toml", toml)
                .part(expected_digest.clone(), part),
        )
        .send()
        .await
        .expect("multipart retry failed");
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::NO_CONTENT,
        "complete package must succeed"
    );

    // The stored deployment now exists and is retrievable.
    let resp = server
        .client
        .get(format!(
            "{}/v1/deployments/{deployment_id}",
            server.base_url
        ))
        .header("Accept", "application/json")
        .send()
        .await
        .expect("get deployment failed");
    assert_eq!(resp.status().as_u16(), 200, "stored deployment must exist");
    let body: Value = resp.json().await.unwrap();
    assert!(
        body["deployment_toml"]
            .as_str()
            .unwrap()
            .contains("content_digest")
    );

    let resp = server
        .client
        .get(format!(
            "{}/v1/deployments/{deployment_id}?include_generated_metadata=false",
            server.base_url
        ))
        .header("Accept", "application/json")
        .send()
        .await
        .expect("get clean deployment failed");
    assert_eq!(resp.status().as_u16(), 200);
    let body: Value = resp.json().await.unwrap();
    let clean = body["deployment_toml"].as_str().unwrap();
    assert!(!clean.contains("content_digest"));

    server.shutdown().await;
}

#[tokio::test]
async fn system_event_retention_dry_run_webapi() {
    use concepts::storage::{SystemEvent, SystemEventCode};

    let server = TestServer::start(test_addr!(40_158)).await;
    let db_pool = SqlitePool::new(&server.sqlite_file, SqliteConfig::default())
        .await
        .unwrap();
    let admin = db_pool.admin_conn().await.unwrap();
    let mut event_ids = Vec::new();
    for age_days in [2, 3] {
        let mut event =
            SystemEvent::new(SystemEventCode::MaintenanceGcFailed, None, None, json!({})).unwrap();
        event.created_at = chrono::Utc::now() - chrono::Duration::days(age_days);
        event_ids.push(event.event_id);
        admin.append_system_event(event).await.unwrap();
    }
    let url = format!("{}/v1/admin/system-events/retain", server.base_url);
    let response = server
        .client
        .post(&url)
        .json(&json!({"max_age_seconds": 86400, "batch_size": 100}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::UNPROCESSABLE_ENTITY);
    for event_id in &event_ids {
        assert!(admin.get_system_event(*event_id).await.unwrap().is_some());
    }

    let response = server
        .client
        .post(&url)
        .json(&json!({"max_age_seconds": 86400, "batch_size": 1, "dry_run": true}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(
        response.json::<Value>().await.unwrap(),
        json!({"deleted": 1, "has_more": true})
    );
    for event_id in &event_ids {
        assert!(admin.get_system_event(*event_id).await.unwrap().is_some());
    }

    let response = server
        .client
        .post(&url)
        .json(&json!({"max_age_seconds": 86400, "batch_size": 100, "dry_run": false}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(
        response.json::<Value>().await.unwrap(),
        json!({"deleted": 2, "has_more": false})
    );
    for event_id in &event_ids {
        assert!(admin.get_system_event(*event_id).await.unwrap().is_none());
    }
    drop(admin);
    db_pool.close().await;
    server.shutdown().await;
}

#[tokio::test]
async fn admin_cleanup_webapi() {
    let server = TestServer::start(test_addr!(40_102)).await;
    let older_execution = server.generate_execution_id().await;
    let newer_execution = server.generate_execution_id().await;
    for execution_id in [&older_execution, &newer_execution] {
        let response = server
            .submit_follow_with_id(
                execution_id,
                "testing:integration/activity.add",
                vec![json!(1), json!(2)],
            )
            .await;
        assert_eq!(response.status(), reqwest::StatusCode::CREATED);
        assert_eq!(response.json::<Value>().await.unwrap(), json!({ "ok": 3 }));
    }

    let response = server
        .client
        .post(format!("{}/v1/admin/executions/retain", server.base_url))
        .json(&json!({ "retain_count": 1, "batch_size": 100, "dry_run": false }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let body: Value = response.json().await.unwrap();
    assert!(
        body["deleted_execution_trees"].as_u64().unwrap() >= 1,
        "{body}"
    );
    assert_eq!(body["has_more"], false);

    let executions = server.list_executions().await;
    let ids = executions
        .as_array()
        .unwrap()
        .iter()
        .map(|execution| execution["execution_id"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert!(!ids.contains(&older_execution.as_str()));
    assert!(ids.contains(&newer_execution.as_str()));

    let response = server
        .client
        .delete(format!(
            "{}/v1/admin/executions/{newer_execution}",
            server.base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(response.json::<Value>().await.unwrap()["deleted"], true);
    let response = server
        .client
        .delete(format!(
            "{}/v1/admin/executions/{newer_execution}",
            server.base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(
        response.json::<Value>().await.unwrap()["already_deleted"],
        true
    );

    let older_deployment = server
        .submit_modified_deployment(|doc| {
            append_inline_stub(
                doc,
                "older-cleanup",
                "testing:integration/stubs.older-cleanup",
            );
        })
        .await;
    let newer_deployment = server
        .submit_modified_deployment(|doc| {
            append_inline_stub(
                doc,
                "newer-cleanup",
                "testing:integration/stubs.newer-cleanup",
            );
        })
        .await;
    let response = server
        .client
        .post(format!("{}/v1/admin/deployments/retain", server.base_url))
        .json(&json!({
            "retain_count": 1,
            "batch_size": 100,
            "delete_executions": false,
            "dry_run": false
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(
        response.json::<Value>().await.unwrap()["deleted_deployments"],
        1
    );
    let response = server
        .client
        .get(format!(
            "{}/v1/deployments/{older_deployment}",
            server.base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);
    let response = server
        .client
        .delete(format!(
            "{}/v1/admin/deployments/{newer_deployment}",
            server.base_url
        ))
        .query(&[("force_non_terminal", true)])
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::PRECONDITION_FAILED);
    let response = server
        .client
        .delete(format!(
            "{}/v1/admin/deployments/{newer_deployment}",
            server.base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(response.json::<Value>().await.unwrap()["deleted"], true);

    server.shutdown().await;
}

#[tokio::test]
async fn shutdown_finishes_with_rest_follow_stream_open() {
    const SHUTDOWN_DEADLINE: Duration = Duration::from_secs(10);
    let server = TestServer::start(test_addr!(129)).await;
    let execution_id = server.generate_execution_id().await;
    let submitted = server
        .submit_paused_webapi(
            &execution_id,
            "testing:integration/workflow-add.add-workflow",
            vec![json!(1), json!(2)],
        )
        .await;
    assert_eq!(submitted.status(), reqwest::StatusCode::CREATED);

    let rest_follow = server
        .client
        .put(format!(
            "{}/v1/executions/{execution_id}?follow=true",
            server.base_url
        ))
        .header("Accept", "application/json")
        .json(&json!({
            "ffqn": "testing:integration/workflow-add.add-workflow",
            "params": [1, 2],
            "paused": true,
        }))
        .send()
        .await
        .expect("REST follow request failed");
    assert_eq!(rest_follow.status(), reqwest::StatusCode::OK);

    // Keep the response body unconsumed to model a slow client with a stream producer in flight.
    let _open_stream = rest_follow;
    server.shutdown_with_timeout(SHUTDOWN_DEADLINE).await;
}

// ---- Activity: submit + result ----

#[rstest::rstest]
#[case::boa_wasm(JsRuntime::BoaWasm)]
#[case::v8(JsRuntime::V8)]
#[tokio::test]
async fn submit_activity_and_get_result(#[case] runtime: JsRuntime) {
    let ip = match runtime {
        JsRuntime::BoaWasm => test_addr!(4),
        JsRuntime::V8 => test_addr!(166),
    };
    let server = TestServer::start_with_js_runtime(ip, runtime.mode()).await;

    let resp = server
        .submit_follow("testing:integration/activity.add", vec![json!(3), json!(5)])
        .await;
    assert_eq!(resp.status().as_u16(), 201);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body, json!({ "ok": 8 }));
    server.shutdown().await;
}

#[tokio::test]
async fn finished_status_follow_is_sse() {
    let server = TestServer::start(test_addr!(40_155)).await;
    let execution_id = server.generate_execution_id().await;
    let result = server
        .submit_follow_with_id(
            &execution_id,
            "testing:integration/activity.add",
            vec![json!(1), json!(2)],
        )
        .await;
    assert_eq!(result.status(), reqwest::StatusCode::CREATED);
    let status = server
        .client
        .get(format!(
            "{}/v1/executions/{execution_id}/status?follow=true&send_finished_status=true",
            server.base_url
        ))
        .header("Accept", "text/event-stream")
        .send()
        .await
        .unwrap();
    assert_eq!(
        status.headers()[reqwest::header::CONTENT_TYPE],
        "text/event-stream"
    );
    let frame = status.text().await.unwrap();
    assert!(frame.contains("event: status\n"));
    assert!(frame.contains("event: finished_status\n"));
    assert!(frame.contains("\"value\":{\"ok\":3}"));
    assert!(frame.contains("id: "));
    assert!(frame.contains("\"pending_state\""));
    let last_id = frame
        .lines()
        .filter_map(|line| line.strip_prefix("id: "))
        .next_back()
        .unwrap();
    let resumed = server
        .client
        .get(format!(
            "{}/v1/executions/{execution_id}/status?follow=true&send_finished_status=true",
            server.base_url,
        ))
        .header("Last-Event-ID", last_id)
        .send()
        .await
        .unwrap();
    assert!(resumed.text().await.unwrap().is_empty());
    let result_resumed = server
        .client
        .get(format!(
            "{}/v1/executions/{execution_id}?follow=true",
            server.base_url,
        ))
        .header("Last-Event-ID", "1")
        .send()
        .await
        .unwrap();
    assert!(result_resumed.text().await.unwrap().is_empty());
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

    let snapshot: Value = server.client.get(format!(
        "{}/v1/executions/{exec_id}/events-and-responses?events_length=100&responses_length=100",
        server.base_url,
    )).send().await.unwrap().json().await.unwrap();
    assert!(snapshot["events"].as_array().unwrap().len() > 1);
    assert!(snapshot["current_status"]["pending_state"].is_object());
    assert!(snapshot["max_version"].is_number());

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

#[tokio::test]
async fn cancel_execution_webapi_routes_activities_and_cancellable_workflows() {
    let server = TestServer::start(test_addr!(84)).await;

    let activity_id = server.generate_execution_id().await;
    server
        .submit_paused_webapi(
            &activity_id,
            "testing:integration/activity.add",
            vec![json!(3), json!(5)],
        )
        .await;
    let resp = server
        .client
        .put(format!(
            "{}/v1/executions/{activity_id}/cancel",
            server.base_url
        ))
        .header("Accept", "application/json")
        .send()
        .await
        .expect("cancel activity request failed");
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(
        resp.json::<Value>().await.unwrap(),
        json!({ "ok": "cancellation requested" })
    );
    // Cancelling a paused activity is async: the driver finalizes it to
    // Finished(Cancelled) on a later tick, so poll rather than asserting immediately.
    while server.get_status(&activity_id).await["pending_state"]["status"] != "finished" {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let cancellable_workflow_id = server
        .seed_cancellable_parent_blocked_on_uncancellable_child()
        .await
        .to_string();
    let resp = server
        .client
        .put(format!(
            "{}/v1/executions/{cancellable_workflow_id}/cancel",
            server.base_url
        ))
        .header("Accept", "application/json")
        .send()
        .await
        .expect("cancel cancellable workflow request failed");
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(
        resp.json::<Value>().await.unwrap(),
        json!({ "ok": "cancellation requested" })
    );
    assert_eq!(
        server.get_status(&cancellable_workflow_id).await["pending_state"]["status"],
        "cancelling"
    );
    let resp = server
        .client
        .put(format!(
            "{}/v1/executions/{cancellable_workflow_id}/cancel",
            server.base_url
        ))
        .header("Accept", "application/json")
        .send()
        .await
        .expect("repeat cancel cancellable workflow request failed");
    assert_eq!(resp.status().as_u16(), 409);
    assert_eq!(
        resp.json::<Value>().await.unwrap(),
        json!({ "err": "already cancelling" })
    );

    let workflow_id = server.generate_execution_id().await;
    server
        .submit_paused_webapi(
            &workflow_id,
            "testing:integration/workflow-add.add-workflow",
            vec![json!(1), json!(2)],
        )
        .await;
    let resp = server
        .client
        .put(format!(
            "{}/v1/executions/{workflow_id}/cancel",
            server.base_url
        ))
        .header("Accept", "application/json")
        .send()
        .await
        .expect("cancel plain workflow request failed");
    assert_eq!(resp.status().as_u16(), 422);
    assert_eq!(
        resp.json::<Value>().await.unwrap(),
        json!({ "err": "cannot cancel, execution is not a cancellable workflow" })
    );

    server.shutdown().await;
}

/// End-to-end: a *running* cancellable workflow, blocked on a durable sleep, is
/// cancelled and the cancellation driver (running no WASM) cancels the pending
/// delay and finishes the workflow as Cancelled.
#[tokio::test]
async fn cancellation_driver_finishes_running_cancellable_workflow_as_cancelled() {
    const FFQN: &str = "testing:integration/workflow-sleep.sleep-cancellable";
    let server = TestServer::start(test_addr!(85)).await;

    // Submit running (not paused); the workflow blocks on a 100s durable sleep.
    let exec_id = server.generate_execution_id().await;
    let resp = server
        .client
        .put(format!("{}/v1/executions/{exec_id}", server.base_url))
        .header("Accept", "application/json")
        .json(&json!({ "ffqn": FFQN, "params": [] }))
        .send()
        .await
        .expect("submit request failed");
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // Wait until it has run and created the delay (blocked, or kept warm-locked).
    while !matches!(
        server.get_status(&exec_id).await["pending_state"]["status"].as_str(),
        Some("blocked_by_join_set" | "locked")
    ) {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let resp = server
        .client
        .put(format!(
            "{}/v1/executions/{exec_id}/cancel",
            server.base_url
        ))
        .header("Accept", "application/json")
        .send()
        .await
        .expect("cancel request failed");
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(
        resp.json::<Value>().await.unwrap(),
        json!({ "ok": "cancellation requested" })
    );

    // The driver drives it to Finished(Cancelled) without the 100s sleep ever expiring.
    let pending_state = loop {
        let status = server.get_status(&exec_id).await;
        if status["pending_state"]["status"] == "finished" {
            break status["pending_state"].clone();
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert_eq!(
        pending_state["result_kind"],
        json!({ "err": { "execution_failure": "cancelled" } }),
        "workflow must finish as Cancelled"
    );

    server.shutdown().await;
}

// ---- Workflow: replaying a paused workflow should return preview events ----

#[tokio::test]
async fn replaying_paused_workflow_should_return_preview_events_webapi() {
    let server = TestServer::start(test_addr!(66)).await;

    let exec_id = server.generate_execution_id().await;
    let submit = server
        .submit_paused_webapi(
            &exec_id,
            "testing:integration/workflow-add-via-activity.add-via-activity",
            vec![json!(3), json!(4)],
        )
        .await;
    assert_eq!(submit.status().as_u16(), 201);

    let replay_resp = server.replay(&exec_id).await;
    assert_eq!(replay_resp.status().as_u16(), 200);
    let replay_body: Value = replay_resp.json().await.unwrap();
    assert_eq!(
        0,
        replay_body["replayed_event_count"]
            .as_u64()
            .expect("replayed_event_count must be a number")
    );
    replay_body["replay_duration_ms"]
        .as_u64()
        .expect("replay_duration_ms must be a number");
    assert_eq!(1, replay_body["replay_version"].as_u64().unwrap());
    assert_eq!(replay_body["type"], "advanceable");
    assert!(
        !replay_body["captured_writes"]
            .as_array()
            .expect("captured_writes must be an array")
            .is_empty(),
        "captured_writes must not be empty for a paused workflow"
    );
    assert_eq!(
        server.persist_backtraces(&exec_id).await,
        0,
        "backtraces for replay-produced future events must be omitted"
    );
    assert_eq!(server.get_backtrace(&exec_id, None).await.status(), 404);

    server.shutdown().await;
}
#[tokio::test]
async fn replay_nondeterminism_reason_survives_webapi() {
    let server = TestServer::start(test_addr!(131)).await;
    let execution_id = server.generate_execution_id().await;
    let submit = server
        .submit_paused_webapi(
            &execution_id,
            "testing:integration/workflow-add-via-activity.add-via-activity",
            vec![json!(3), json!(4)],
        )
        .await;
    assert_eq!(submit.status().as_u16(), 201);

    let replay = server.replay(&execution_id).await;
    assert_eq!(replay.status().as_u16(), 200);
    let mut replay: Value = replay.json().await.unwrap();
    let captured_writes = replay["captured_writes"]
        .as_array_mut()
        .expect("captured_writes must be an array");
    let advance = server
        .client
        .put(format!(
            "{}/v1/executions/{execution_id}/advance",
            server.base_url
        ))
        .header("Accept", "application/json")
        .json(&json!({
            "captured_writes": captured_writes,
            "persist_backtrace": false,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(advance.status().as_u16(), 200);
    server.corrupt_child_request_params_hash(&execution_id);

    let failed = server.replay(&execution_id).await;
    assert_eq!(failed.status().as_u16(), 409);
    let failed: Value = failed.json().await.unwrap();
    assert_eq!(failed["type"], "replay_failed");
    assert_eq!(failed["failure"]["kind"], "nondeterminism_detected");
    let detail = failed["failure"]["detail"]
        .as_str()
        .expect("nondeterminism detail must be set");
    assert!(
        detail.contains("found unprocessed request stored at version 3")
            && detail.contains("testing:integration/activity.add"),
        "unexpected nondeterminism detail: {detail}"
    );
    server.shutdown().await;
}

#[tokio::test]
async fn replay_and_advance_paused_js_workflow_until_finished_webapi() {
    let server = TestServer::start(test_addr!(65)).await;
    let stepped = server
        .step_execution_until_finished_webapi(
            "testing:integration/workflow-call-stub.call-stub",
            vec![json!(123_u64)],
        )
        .await;
    assert!(
        stepped.steps > 0,
        "step-through harness must execute at least one replay+advance round"
    );
    assert_eq!(stepped.retval, json!({"ok":"stub-ok"}));
    server.shutdown().await;
}

#[tokio::test]
async fn replay_finished_execution_reports_final_version_webapi() {
    let server = TestServer::start(test_addr!(134)).await;
    let stepped = server
        .step_execution_until_finished_webapi(
            "testing:integration/workflow-math-random.math-random",
            vec![],
        )
        .await;
    assert_eq!(1, stepped.steps);

    let replay = server.replay(&stepped.execution_id).await;
    assert_eq!(replay.status().as_u16(), 200);
    let replay: ReplayResponseSer = replay.json().await.unwrap();
    assert_eq!(5, replay.replay_version);
    assert!(matches!(replay.outcome, ReplayOutcomeSer::Finished { .. }));
    server.shutdown().await;
}

#[tokio::test]
async fn replay_and_advance_oversized_result_webapi() {
    let server = TestServer::start_with_server_lines_and_component(
        test_addr!(127),
        "limits.max_persisted_value_size_bytes = 256",
        "workflow_js",
        "test_make_record_workflow",
    )
    .await;
    let stepped = server
        .step_execution_until_finished_webapi(
            "testing:integration/workflow-make-record.make-record",
            vec![json!("x".repeat(240))],
        )
        .await;
    assert_eq!(stepped.steps, 1);
    assert_eq!(
        stepped.retval,
        json!({
            "execution_failed": {
                "kind": "value_too_large",
                "reason": "function result exceeds the persisted value limit",
                "detail": "limit: 256 bytes"
            }
        })
    );
    server.shutdown().await;
}

#[tokio::test]
async fn replay_after_restart_uses_original_persisted_value_limit() {
    let server = TestServer::start_with_server_lines_and_component(
        test_addr!(128),
        "limits.max_persisted_value_size_bytes = 256",
        "workflow_js",
        "test_make_record_workflow",
    )
    .await;
    let execution_id = server.generate_execution_id().await;
    let submit = server
        .submit_paused_webapi(
            &execution_id,
            "testing:integration/workflow-make-record.make-record",
            vec![json!("x".repeat(240))],
        )
        .await;
    assert_eq!(submit.status(), reqwest::StatusCode::CREATED);

    let server = server.restart_with_persisted_value_limit(1024).await;
    let replay = server.replay(&execution_id).await;
    assert_eq!(replay.status(), reqwest::StatusCode::OK);
    let replay: Value = replay.json().await.unwrap();
    assert_eq!(replay["type"], "advanceable");
    let captured_writes = replay["captured_writes"].clone();
    assert!(!captured_writes.as_array().unwrap().is_empty());

    let advance = server
        .client
        .put(format!(
            "{}/v1/executions/{execution_id}/advance",
            server.base_url
        ))
        .header("Accept", "application/json")
        .json(&json!({
            "captured_writes": captured_writes,
            "persist_backtrace": true,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(advance.status(), reqwest::StatusCode::OK);
    assert_eq!(
        advance.json::<Value>().await.unwrap(),
        json!({
            "type": "finished",
            "value": {
                "execution_failed": {
                    "kind": "value_too_large",
                    "reason": "function result exceeds the persisted value limit",
                    "detail": "limit: 256 bytes"
                }
            }
        })
    );

    server.shutdown().await;
}

// ---- Workflow: replay failed (type mismatch) with advance --force ----

const REPLAY_FAILED_FFQN: &str = "testing:integration/workflow-return-wrong-type.return-wrong-type";

#[tokio::test]
async fn replay_failed_js_workflow_then_advance_webapi() {
    let server = TestServer::start(test_addr!(67)).await;
    let stepped = server
        .step_execution_until_finished_webapi(REPLAY_FAILED_FFQN, vec![])
        .await;
    assert_eq!(stepped.steps, 1);
    assert_eq!(
        json!({
            "execution_failed": {
                "kind":"uncategorized",
                "reason":"value does not type check - failed to type check the ok variant value `\"not-a-number\"` as type u32 - invalid type: string \"not-a-number\", expected value matching \"u32\" at line 1 column 14"
            }
        }),
        stepped.retval
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
    assert_eq!(body, json!({ "ok": 15 }));

    let events = server.get_events(&exec_id).await;
    let events = sanitize_json(&events);
    insta::assert_json_snapshot!("workflow_add_via_activity_events", events);
    server.shutdown().await;
}

#[tokio::test]
async fn typed_js_submit_throws_when_child_params_exceed_persisted_value_limit() {
    let server = TestServer::start_with_server_lines(
        test_addr!(150),
        "limits.max_persisted_value_size_bytes = 256",
    )
    .await;
    let exec_id = server.generate_execution_id().await;

    let resp = server
        .submit_follow_with_id(
            &exec_id,
            "testing:integration/workflow-import-ext-submit-error.submit-error",
            vec![],
        )
        .await;
    assert_eq!(resp.status().as_u16(), 201);
    assert_eq!(
        resp.json::<Value>().await.unwrap(),
        json!({ "ok": "typed submit threw" })
    );

    server.shutdown().await;
}

#[tokio::test]
async fn submit_scheduled_execution_via_schedule_extension() {
    let server = TestServer::start(test_addr!(76)).await;

    let resp = server
        .client
        .post(format!("{}/v1/executions", server.base_url))
        .header("Accept", "application/json")
        .json(&json!({
            "ffqn": "testing:integration-obelisk-schedule/activity.add-schedule",
            "params": [
                { "in": { "seconds": 60 } },
                3,
                4
            ]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 201);

    let body: Value = resp.json().await.unwrap();
    let scheduled_exec_id = body["ok"]
        .as_str()
        .expect("expected scheduled execution ID in ok result");
    assert!(
        scheduled_exec_id.starts_with("E_"),
        "expected execution ID, got: {scheduled_exec_id}"
    );

    let status = server.get_status(scheduled_exec_id).await;
    assert_eq!(status["ffqn"], json!("testing:integration/activity.add"));
    assert_eq!(status["pending_state"]["status"], json!("pending_at"));

    server.shutdown().await;
}

// ---- Execution listing ----

#[tokio::test]
async fn list_executions_after_submit() {
    let server = TestServer::start(test_addr!(11)).await;

    let resp = server
        .submit_follow("testing:integration/activity.add", vec![json!(1), json!(2)])
        .await;
    assert_eq!(resp.status().as_u16(), 201);
    let _: Value = resp.json().await.unwrap();

    let executions = server.list_executions().await;
    let arr = executions.as_array().expect("array");
    assert_eq!(arr.len(), 1, "unexpected {arr:?}");
    assert_eq!(
        arr[0]["ffqn"],
        json!("testing:integration/activity.add"),
        "unexpected {arr:?}"
    );
    let filtered: Value = server
        .client
        .get(format!(
            "{}/v1/executions?package=testing:integration&state=finished_error&state=finished_ok",
            server.base_url,
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(filtered.as_array().unwrap().len(), 1);
    let wrong_scope: Value = server
        .client
        .get(format!(
            "{}/v1/executions?interface=testing:integration/activity2",
            server.base_url,
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(wrong_scope.as_array().unwrap().is_empty());
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
            "ffqn": "testing:integration/activity.add",
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
async fn rest_submit_enforces_exact_persisted_value_boundary() {
    let server = TestServer::start_with_server_lines_and_component(
        test_addr!(125),
        "limits.max_persisted_value_size_bytes = 16",
        "activity_exec",
        "testing:integration/exec-greet.greet-inline",
    )
    .await;
    let ffqn = "testing:integration/exec-greet.greet-inline";

    let accepted_id = server.generate_execution_id().await;
    let accepted = server
        .submit_paused_webapi(&accepted_id, ffqn, vec![json!("secretmarker")])
        .await;
    assert_eq!(accepted.status(), reqwest::StatusCode::CREATED);

    let rejected_id = server.generate_execution_id().await;
    let rejected = server
        .submit_paused_webapi(&rejected_id, ffqn, vec![json!("secretmarkerx")])
        .await;
    assert_eq!(rejected.status(), reqwest::StatusCode::PAYLOAD_TOO_LARGE);
    let error = rejected.json::<Value>().await.unwrap();
    assert_eq!(
        error,
        json!({ "err": "execution parameters exceed the 16-byte persisted value limit" })
    );
    assert!(!error.to_string().contains("secretmarkerx"));
    assert_eq!(server.list_executions().await.as_array().unwrap().len(), 1);

    server.shutdown().await;
}

#[tokio::test]
async fn replay_nonexistent_execution_returns_404() {
    let server = TestServer::start(test_addr!(14)).await;
    let resp = server.replay("E_01AAAAAAAAAAAAAAAAAAAAAAAA").await;
    assert_eq!(resp.status().as_u16(), 404);
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
            "testing:integration/activity.add",
            vec![json!(1), json!(2)],
        )
        .await;
    assert_eq!(resp1.status().as_u16(), 201);
    let body1: Value = resp1.json().await.unwrap();

    let resp2 = server
        .submit_follow_with_id(
            &exec_id,
            "testing:integration/activity.add",
            vec![json!(1), json!(2)],
        )
        .await;
    assert_eq!(resp2.status().as_u16(), 200);
    let body2: Value = resp2.json().await.unwrap();
    assert_eq!(body1, body2);
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
async fn hot_redeploy_activity_impl(server: &TestServer) {
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
    server
        .submit_and_hot_redeploy(|doc| {
            set_component_env_var(
                doc,
                "activity_js",
                "test_read_env_activity",
                "TEST_ENV_VAR",
                "updated_value",
            );
        })
        .await;

    // 3. Run the activity again — must return the updated value.
    let resp = server
        .submit_follow(
            "testing:integration/activity-env.read-env",
            vec![json!("TEST_ENV_VAR")],
        )
        .await;
    assert_eq!(resp.status().as_u16(), 201);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body, json!({"ok": "updated_value"}));
}

#[rstest::rstest]
#[case::boa_wasm(JsRuntime::BoaWasm)]
#[case::v8(JsRuntime::V8)]
#[tokio::test]
async fn hot_redeploy_activity(#[case] runtime: JsRuntime) {
    let ip = match runtime {
        JsRuntime::BoaWasm => test_addr!(39),
        JsRuntime::V8 => test_addr!(168),
    };
    let server = TestServer::start_with_js_runtime(ip, runtime.mode()).await;
    hot_redeploy_activity_impl(&server).await;
    server.shutdown().await;
}

/// After a hot redeploy, the web API server must expose the updated component registry.
///
/// A new inline stub `testing:integration/stubs.new-hot-stub` is added only in
/// the second deployment. The test asserts that the stub is absent from REST
/// `/v1/functions` before the hot redeploy and present after it.
async fn hot_redeploy_registry_impl(server: &TestServer) {
    const NEW_STUB_FFQN: &str = "testing:integration/stubs.new-hot-stub";

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

    assert!(
        !rest_has_new_stub().await,
        "stub must be absent before hot redeploy"
    );
    server
        .submit_and_hot_redeploy(|doc| {
            append_inline_stub(doc, "new_hot_stub", NEW_STUB_FFQN);
        })
        .await;
    assert!(
        rest_has_new_stub().await,
        "stub must be present after hot redeploy"
    );
}

#[tokio::test]
async fn hot_redeploy_registry_webapi() {
    let server = TestServer::start(test_addr!(40)).await;
    hot_redeploy_registry_impl(&server).await;
    server.shutdown().await;
}

/// After a hot redeploy, an existing JS webhook endpoint must pick up the new
/// env-var value from the updated deployment — proving that `WebhookServerState`
/// (including `fn_registry`, `deployment_id`, and the rebuilt router) is pushed
/// through the `WebhookRegistry` watch channel.
async fn hot_redeploy_webhook_js_env_var_impl(server: &TestServer) {
    // 1. Verify the initial env var value is served.
    let resp = server
        .client
        .get(format!("{}/read-env", server.webhook_base_url))
        .send()
        .await
        .expect("webhook request failed");
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(resp.text().await.unwrap(), "hello_from_webhook_env");

    // 2. Build a second deployment with the env var changed and hot-redeploy.
    server
        .submit_and_hot_redeploy(|doc| {
            set_component_env_var(
                doc,
                "webhook_endpoint_js",
                "test_read_env_webhook",
                "WEBHOOK_TEST_ENV_VAR",
                "updated_webhook_env",
            );
        })
        .await;
    debug!("Expecting new deployment to answer the next request");
    let resp = server
        .client
        .get(format!("{}/read-env", server.webhook_base_url))
        .send()
        .await
        .expect("webhook request failed after hot redeploy");
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(resp.text().await.unwrap(), "updated_webhook_env");
}

#[rstest::rstest]
#[case::boa_wasm(JsRuntime::BoaWasm)]
#[case::v8(JsRuntime::V8)]
#[tokio::test]
async fn hot_redeploy_webhook_js_env_var(#[case] runtime: JsRuntime) {
    let ip = match runtime {
        JsRuntime::BoaWasm => test_addr!(41),
        JsRuntime::V8 => test_addr!(170),
    };
    let server = TestServer::start_with_js_runtime(ip, runtime.mode()).await;
    hot_redeploy_webhook_js_env_var_impl(&server).await;
    server.shutdown().await;
}

/// After a hot redeploy that removes a JS webhook endpoint, the route must
/// return 404 — proving that the router inside the running HTTP server is
/// replaced, not just the env vars.
async fn hot_redeploy_webhook_js_remove_endpoint_impl(server: &TestServer) {
    // 1. Verify /hello is served by the initial deployment.
    let resp = server
        .client
        .get(format!("{}/hello", server.webhook_base_url))
        .send()
        .await
        .expect("webhook request failed");
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(resp.text().await.unwrap(), "Hello from JS webhook!");

    // 2. Build a second deployment that removes test_hello_webhook and hot-redeploy.
    server
        .submit_and_hot_redeploy(|doc| {
            remove_component(doc, "webhook_endpoint_js", "test_hello_webhook");
        })
        .await;

    // 3. /hello must now return 404 — the endpoint was removed from the router.
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
}

#[rstest::rstest]
#[case::boa_wasm(JsRuntime::BoaWasm)]
#[case::v8(JsRuntime::V8)]
#[tokio::test]
async fn hot_redeploy_webhook_js_remove_endpoint(#[case] runtime: JsRuntime) {
    let ip = match runtime {
        JsRuntime::BoaWasm => test_addr!(42),
        JsRuntime::V8 => test_addr!(172),
    };
    let server = TestServer::start_with_js_runtime(ip, runtime.mode()).await;
    hot_redeploy_webhook_js_remove_endpoint_impl(&server).await;
    server.shutdown().await;
}

// ---- Backtrace API ----

#[rstest::rstest]
#[case::boa_wasm(JsRuntime::BoaWasm)]
#[case::v8(JsRuntime::V8)]
#[tokio::test]
async fn backtrace_workflow_calling_activity(#[case] runtime: JsRuntime) {
    let ip = match runtime {
        JsRuntime::BoaWasm => test_addr!(35),
        JsRuntime::V8 => test_addr!(175),
    };
    let server = TestServer::start_with_js_runtime(ip, runtime.mode()).await;
    let exec_id = server.generate_execution_id().await;

    // Run a workflow that calls a child activity.
    let resp = server
        .submit_follow_with_id(
            &exec_id,
            "testing:integration/workflow-call-activity.call-activity",
            vec![json!(3), json!(4)],
        )
        .await;
    assert_eq!(resp.status().as_u16(), 201);
    let _: Value = resp.json().await.unwrap(); // consume body

    let resp = server.get_backtrace(&exec_id, None).await;
    assert_eq!(
        resp.status().as_u16(),
        404,
        "regular execution must not persist backtraces"
    );

    let replay = server.replay(&exec_id).await;
    assert_eq!(replay.status().as_u16(), 200);
    let _: Value = replay.json().await.unwrap();
    let resp = server.get_backtrace(&exec_id, None).await;
    assert_eq!(
        resp.status().as_u16(),
        404,
        "replay must not persist backtraces"
    );

    assert!(server.persist_backtraces(&exec_id).await > 0);
    assert_eq!(
        server.persist_backtraces(&exec_id).await,
        0,
        "persisting the same backtraces again must be idempotent"
    );

    // Default (last) filter.
    let resp = server.get_backtrace(&exec_id, None).await;
    assert_eq!(resp.status().as_u16(), 200, "backtrace should exist");
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["execution_id"], json!(exec_id));
    assert!(
        body["component_id"].is_object(),
        "component_id must be a non-empty object"
    );
    assert!(
        body["version_min_including"].is_number(),
        "version_min_including must be a number"
    );
    assert!(
        body["version_max_excluding"].is_number(),
        "version_max_excluding must be a number"
    );
    assert_eq!(
        body["version_max_excluding"].as_u64().unwrap()
            - body["version_min_including"].as_u64().unwrap(),
        3,
        "a direct call backtrace must cover its three history events"
    );
    // The debugger resolves a source by the frame's file and highlights its line, so both
    // runtimes must point at the `dynamic.call` call site in `call_activity.js`.
    let symbol = body["wasm_backtrace"]["frames"][0]["symbols"][0].clone();
    assert_eq!(
        symbol["file"],
        json!("call_activity.js"),
        "top frame must name the JS source file: {body}"
    );
    assert_eq!(
        symbol["line"],
        json!(5),
        "top frame must point at the `dynamic.call` line: {body}"
    );

    // ?version=first should also succeed and return a consistent structure.
    let resp_first = server.get_backtrace(&exec_id, Some("first")).await;
    assert_eq!(
        resp_first.status().as_u16(),
        200,
        "version=first should work"
    );
    let body_first: Value = resp_first.json().await.unwrap();
    assert_eq!(body_first["execution_id"], json!(exec_id));

    // ?version=<version_min_including> (numeric) should return the same record.
    let version_num = body["version_min_including"].as_u64().unwrap();
    let resp_num = server
        .get_backtrace(&exec_id, Some(&version_num.to_string()))
        .await;
    assert_eq!(
        resp_num.status().as_u16(),
        200,
        "numeric version matching stored version should work"
    );

    // Invalid version string must return 400.
    let resp_bad = server.get_backtrace(&exec_id, Some("bogus")).await;
    assert_eq!(
        resp_bad.status().as_u16(),
        400,
        "invalid version must be 400"
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

#[rstest::rstest]
#[case::boa_wasm(JsRuntime::BoaWasm)]
#[case::v8(JsRuntime::V8)]
#[tokio::test]
async fn backtrace_source_workflow_calling_activity(#[case] runtime: JsRuntime) {
    let ip = match runtime {
        JsRuntime::BoaWasm => test_addr!(36),
        JsRuntime::V8 => test_addr!(176),
    };
    let server = TestServer::start_with_js_runtime(ip, runtime.mode()).await;
    let exec_id = server.generate_execution_id().await;

    // Run and replay the workflow to populate lazy backtraces.
    let resp = server
        .submit_follow_with_id(
            &exec_id,
            "testing:integration/workflow-add-via-activity.add-via-activity",
            vec![json!(2), json!(3)],
        )
        .await;
    assert_eq!(resp.status().as_u16(), 201);
    let _: Value = resp.json().await.unwrap();
    assert!(server.persist_backtraces(&exec_id).await > 0);

    // The frames must name the very source the endpoint below serves.
    let backtrace: Value = server
        .get_backtrace(&exec_id, None)
        .await
        .json()
        .await
        .unwrap();
    assert_eq!(
        backtrace["wasm_backtrace"]["frames"][0]["symbols"][0]["file"],
        json!("add_via_activity.js"),
        "top frame must name the JS source file: {backtrace}"
    );

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

    // ?version=first should resolve the same component and return the same source.
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
