//! Integration tests for the Web API.
//!
//! Each test spins up a full Obelisk server (compile → DB → HTTP) against a
//! temporary TOML config that references the JS test-program fixtures.
//!
//! Each test uses a unique loopback address in the 127.1.0.0/16 range with
//! fixed ports, allowing parallel test execution without conflicts.
//! The `test_addr!` macro ensures unique addresses at link time.

use crate::server::web_api_server::ReplayResponseSer;
use crate::{
    command::server::{LocalDeployment, PrepareDirsParams, RunParams, prepare_dirs, run_internal},
    config::config_holder::ConfigHolder,
};
use toml_edit::{DocumentMut, value};

/// Append an inline `[[activity_stub]]` to a deployment manifest, mirroring the canonical
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
use grpc::grpc_gen::{
    AdvanceExecutionRequest, CancelExecutionRequest, DeploymentId as GrpcDeploymentId,
    ExecutionId as GrpcExecutionId, GcOrphanFilesRequest, GetDeploymentRequest, GetFileRequest,
    GetStatusRequest, ListComponentsRequest, ReplayExecutionRequest, RuntimeConfigCheck,
    SubmitDeploymentRequest, SubmitRequest, SwitchDeploymentRequest,
    cancel_execution_response::CancelExecutionOutcome,
    deployment_repository_client::DeploymentRepositoryClient,
    execution_repository_client::ExecutionRepositoryClient,
    function_repository_client::FunctionRepositoryClient, switch_deployment_response::Outcome,
};
use hmac::{Hmac, Mac};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::Sha256;
use std::fmt::Write as _;
use std::{
    path::{Path, PathBuf},
    time::Duration,
};
use test_utils::sanitize_json;
use tokio::{sync::watch, task::JoinHandle};
use tokio_stream::StreamExt;
use tracing::{debug, info, instrument};

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
    let fixture_src = workspace.join("crates/testing/test-programs");
    let fixture_dst = db_dir.path().join("crates/testing/test-programs");
    copy_dir_recursive(&fixture_src.join("js"), &fixture_dst.join("js"));
    copy_dir_recursive(&fixture_src.join("exec"), &fixture_dst.join("exec"));
    let server_contents = format!(
        r#"api.listening_addr = "{ip}:{API_PORT}"
webui.enabled = false
external.listening_addr = "{ip}:{WEBHOOK_PORT}"

[wasm.codegen_cache]
directory = "{codegen_cache}"

[database.sqlite]
directory = "{db_dir}"
"#,
        ip = ip,
        API_PORT = API_PORT,
        WEBHOOK_PORT = WEBHOOK_PORT,
        codegen_cache = workspace.join("test-codegen-cache").display(),
        db_dir = db_dir.path().display(),
    );
    let server_path = db_dir.path().join("obelisk-test-server.toml");
    std::fs::write(&server_path, server_contents).unwrap();

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
name = "test_greet_activity"
location = "{ws}/crates/testing/test-programs/js/activity/greet.js"
ffqn = "testing:integration/activity-greet.greet"
params = [
  {{ name = "name", type = "string" }},
]
return_type = "result<string, string>"

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

[[activity_js]]
name = "test_hmac_sign_verify_activity"
location = "{ws}/crates/testing/test-programs/js/activity/hmac_sign_verify.js"
ffqn = "testing:integration/activity-hmac.hmac-sign-verify"
params = [
  {{ name = "key", type = "string" }},
  {{ name = "message", type = "string" }},
]
return_type = "result<string, string>"

[[activity_exec]]
ffqn = "testing:integration/exec-add.add"
location = "{ws}/crates/testing/test-programs/exec/add.sh"
params = [
  {{ name = "a", type = "u32" }},
  {{ name = "b", type = "u32" }},
]
return_type = "result<u32, string>"

[[activity_exec]]
ffqn = "testing:integration/exec-greet.greet-include"
location = "{ws}/crates/testing/test-programs/exec/greet.sh"
params = [
  {{ name = "name", type = "string" }},
]
return_type = "result<string, string>"
env_vars = ["PATH"] # for jq

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

[[activity_exec]]
ffqn = "testing:integration/exec-env.read-env"
location = "{ws}/crates/testing/test-programs/exec/read-env.sh"
return_type = "result<string, string>"
env_vars = [{{key = "MY_VAR", value = "hello_from_exec_env"}}]

[[activity_exec]]
ffqn = "testing:integration/exec-error.fail"
content = '''#!/usr/bin/env bash
echo '"something went wrong"'
exit 1
'''
return_type = "result<string, string>"

[[activity_exec]]
ffqn = "testing:integration/exec-record.make-record"
content = '''#!/usr/bin/env bash
printf '{{"name": "Alice", "count": 42}}'
'''
return_type = "result<record {{ name: string, count: u32 }}, string>"

[[activity_exec]]
ffqn = "testing:integration/exec-stdin.expose-secrets"
location = "{ws}/crates/testing/test-programs/exec/expose-secrets.sh"
return_type = "result<string, string>"
env_vars = ["PATH"] # for jq
[activity_exec.secrets]
env_vars = [{{ name = "MY_SECRET", value = "s3cret_value" }}]

[[activity_exec]]
content = '''#!/bin/sh
true
'''
ffqn = "testing:integration/exec-void.void-ok"
return_type = "result"

[[activity_exec]]
content = '''#!/bin/sh
false
'''
ffqn = "testing:integration/exec-void.void-err"

[[activity_exec]]
ffqn = "testing:integration/exec-args.echo-args"
content = '''#!/usr/bin/env bash
# Receives two u32 params as JSON args: $1 and $2
printf '{{"a": %s, "b": %s}}' "$1" "$2"
'''
params = [
  {{ name = "a", type = "u32" }},
  {{ name = "b", type = "u32" }},
]
return_type = "result<record {{ a: u32, b: u32 }}, string>"

[[activity_exec]]
ffqn = "testing:integration/exec-stdin-args.echo-args"
content = '''#!/usr/bin/env bash
set -euo pipefail
# Receives params via the stdin JSON `params` array instead of argv.
jq -c '{{a: .params[0], b: .params[1]}}' /dev/stdin
'''
params = [
  {{ name = "a", type = "u32" }},
  {{ name = "b", type = "u32" }},
]
return_type = "result<record {{ a: u32, b: u32 }}, string>"
env_vars = ["PATH"] # for jq
params_via_stdin = true

[[activity_exec]]
ffqn = "testing:integration/exec-stream.stream-test"
content = '''#!/usr/bin/env bash
echo "line1" >&2
sleep 0.1
echo "line2" >&2
'''
env_vars = ["PATH"] # for sleep

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
name = "test_get_status_webhook"
location = "{ws}/crates/testing/test-programs/js/webhook/get_status.js"
routes = [{{ methods = ["GET"], route = "/get-status" }}]

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
        let (tmp_dir, server_path, deployment_path) = write_test_configs(&ip);
        let deployment = LocalDeployment::from_path(&deployment_path).await.unwrap();
        Self::launch(ip, tmp_dir, server_path, deployment).await
    }

    /// Start a server with an empty deployment (no components, no CAS blobs), so a later
    /// `deployment apply` exercises the upload-then-submit path from a clean store.
    async fn start_empty(ip: String) -> Self {
        let (tmp_dir, server_path, _deployment_path) = write_test_configs(&ip);
        Self::launch(ip, tmp_dir, server_path, LocalDeployment::empty()).await
    }

    async fn launch(
        ip: String,
        tmp_dir: tempfile::TempDir,
        server_path: PathBuf,
        deployment: LocalDeployment,
    ) -> Self {
        test_utils::set_up();

        let project_dirs = crate::project_dirs();
        let base_dirs = BaseDirs::new();
        let config_holder = ConfigHolder::new(project_dirs, base_dirs, Some(server_path)).unwrap();
        let config = config_holder.load_config().await.unwrap();

        let (termination_sender, termination_watcher) = watch::channel(());

        let params = RunParams {
            dir_params: PrepareDirsParams::default(),
            clean_sqlite_directory: false,
            suppress_type_checking_errors: false,
        };

        let prepared_dirs = prepare_dirs(&config, &params.dir_params, &config_holder.path_prefixes)
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
                && resp.status().is_success()
            {
                debug!("Pinging server OK");
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

    async fn grpc_list_components(
        &self,
        deployment_id: Option<DeploymentId>,
    ) -> grpc::grpc_gen::ListComponentsResponse {
        let mut fn_client =
            FunctionRepositoryClient::connect(format!("http://{}", self.api_addr()))
                .await
                .unwrap();
        fn_client
            .list_components(ListComponentsRequest {
                function_name: None,
                component_digest: None,
                extensions: false,
                deployment_id: deployment_id.map(|deployment_id| GrpcDeploymentId {
                    id: deployment_id.to_string(),
                }),
            })
            .await
            .unwrap()
            .into_inner()
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
            .json(&json!({ "deployment_toml": deployment_toml }))
            .send()
            .await
            .expect("webapi submit deployment request failed");
        assert!(
            resp.status().is_success(),
            "webapi submit deployment failed: {}",
            resp.status()
        );
        let body: Value = resp.json().await.unwrap();
        body["deployment_id"]
            .as_str()
            .expect("webapi submit deployment: missing deployment_id field")
            .parse()
            .expect("webapi submit deployment: invalid deployment id")
    }

    /// Submit a deployment via the Web API with an explicit (idempotency) deployment ID,
    /// returning the raw response so callers can assert on success or conflict.
    async fn webapi_submit_deployment_with_id(
        &self,
        deployment_toml: &str,
        deployment_id: DeploymentId,
    ) -> reqwest::Response {
        self.client
            .post(format!("{}/v1/deployments", self.base_url))
            .header("Accept", "application/json")
            .json(&json!({
                "deployment_toml": deployment_toml,
                "deployment_id": deployment_id.to_string(),
            }))
            .send()
            .await
            .expect("webapi submit deployment request failed")
    }

    /// Submit a manifest over gRPC as a CAS-efficient package: preflight without
    /// blobs, then retry attaching only the files the server reports missing.
    /// Returns the new deployment ID.
    async fn grpc_upload_and_submit_manifest(
        &self,
        deployment_toml_path: &std::path::Path,
    ) -> DeploymentId {
        use prost::Message as _;

        let prepared =
            crate::config::manifest::prepare_deployment_manifest_from_disk(deployment_toml_path)
                .await
                .expect("cannot prepare deployment manifest");
        let grpc_client =
            DeploymentRepositoryClient::connect(format!("http://{}", self.api_addr()))
                .await
                .unwrap()
                .max_encoding_message_size(crate::MAX_GRPC_MESSAGE_SIZE)
                .max_decoding_message_size(crate::MAX_GRPC_MESSAGE_SIZE);

        let submit = async |files| {
            grpc_client
                .clone()
                .submit_deployment(SubmitDeploymentRequest {
                    deployment_toml: prepared.deployment_toml.clone(),
                    created_by: Some("test".to_string()),
                    runtime_config_check: RuntimeConfigCheck::Strict as i32,
                    description: None,
                    deployment_id: None,
                    files,
                })
                .await
        };

        // Preflight: no blobs attached. Succeeds outright when every referenced
        // digest is already in the CAS.
        let resp = match submit(Vec::new()).await {
            Ok(resp) => resp.into_inner(),
            Err(status) => {
                let detail = grpc::grpc_gen::SubmitDeploymentErrorDetail::decode(status.details())
                    .expect("submit error must carry SubmitDeploymentErrorDetail");
                let missing: Vec<String> = detail
                    .missing_files
                    .iter()
                    .filter_map(|issue| issue.digest.clone())
                    .collect();
                // Retry with only the missing blobs.
                let files = prepared
                    .files
                    .iter()
                    .filter(|file| missing.contains(&file.digest.to_string()))
                    .map(|file| grpc::grpc_gen::DeploymentFileContent {
                        path: file.path.clone(),
                        digest: Some(file.digest.to_string()),
                        content: file.bytes.clone(),
                    })
                    .collect();
                submit(files)
                    .await
                    .expect("resubmit with blobs failed")
                    .into_inner()
            }
        };
        resp.deployment_id
            .expect("submit_deployment: missing deployment_id")
            .id
            .parse()
            .expect("submit_deployment: invalid deployment id")
    }

    /// Hot-redeploy to the given deployment over gRPC, asserting the switch succeeded.
    async fn grpc_switch_hot_redeploy(&self, deployment_id: DeploymentId) {
        let mut grpc_client =
            DeploymentRepositoryClient::connect(format!("http://{}", self.api_addr()))
                .await
                .unwrap()
                .max_encoding_message_size(crate::MAX_GRPC_MESSAGE_SIZE)
                .max_decoding_message_size(crate::MAX_GRPC_MESSAGE_SIZE);
        let resp = grpc_client
            .switch_deployment(SwitchDeploymentRequest {
                deployment_id: Some(GrpcDeploymentId {
                    id: deployment_id.to_string(),
                }),
                runtime_config_check: RuntimeConfigCheck::Strict as i32,
                hot_redeploy: true,
            })
            .await
            .expect("switch_deployment failed")
            .into_inner();
        assert_eq!(resp.outcome(), Outcome::SwitchOutcomeSwitched);
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
            .json(&json!({ "hot_redeploy": true }))
            .send()
            .await
            .expect("webapi switch deployment request failed");
        assert!(
            resp.status().is_success(),
            "webapi switch deployment failed: {}",
            resp.status()
        );
        let body: Value = resp.json().await.unwrap();
        assert_eq!(body["ok"], "switched", "unexpected switch outcome");
    }
}

/// Regression test for hot-deploying local WASM files to a server that starts empty.
///
/// Starts with an empty deployment (no components, no CAS blobs), then runs the
/// `deployment apply` flow over gRPC: upload every referenced WASM blob, submit the verbatim
/// manifest, and hot-redeploy. This used to fail because the server canonicalized the stored
/// manifest against a synthetic `/deployment` root and then tried to read the WASM from that
/// non-existent path instead of materializing the uploaded blobs from the CAS.
#[tokio::test]
async fn deploy_local_wasm_to_empty_server() {
    let server = TestServer::start_empty(test_addr!(78)).await;

    let toml_path = get_workspace_dir().join("obelisk-testing-wasm-local.toml");
    let deployment_id = server.grpc_upload_and_submit_manifest(&toml_path).await;
    server.grpc_switch_hot_redeploy(deployment_id).await;

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

/// Selects which protocol to use for submit + hot-redeploy in parametrized tests.
enum TestDeployClient {
    /// Submit and switch via gRPC.
    Grpc,
    /// Submit and switch via the Web API.
    WebApi,
}

impl TestDeployClient {
    /// Read the active deployment config from `SQLite`, apply `mutate` to it,
    /// then submit the modified deployment and hot-redeploy to it using
    /// whichever protocol this client represents.
    #[instrument(skip_all)]
    async fn submit_and_hot_redeploy(
        &self,
        server: &TestServer,
        mutate: impl FnOnce(&mut DocumentMut),
    ) {
        let mut doc = server
            .active_deployment_toml()
            .await
            .parse::<DocumentMut>()
            .unwrap();
        mutate(&mut doc);
        let new_deployment_toml = doc.to_string();

        match self {
            TestDeployClient::Grpc => {
                let mut grpc_client =
                    DeploymentRepositoryClient::connect(format!("http://{}", server.api_addr()))
                        .await
                        .unwrap()
                        .max_encoding_message_size(crate::MAX_GRPC_MESSAGE_SIZE)
                        .max_decoding_message_size(crate::MAX_GRPC_MESSAGE_SIZE);
                let submit_resp = grpc_client
                    .submit_deployment(SubmitDeploymentRequest {
                        deployment_toml: new_deployment_toml,
                        created_by: Some("test".to_string()),
                        runtime_config_check: RuntimeConfigCheck::Strict as i32,
                        description: None,
                        deployment_id: None,
                        // The mutated active manifest references only blobs already in the CAS.
                        files: Vec::new(),
                    })
                    .await
                    .unwrap()
                    .into_inner();
                let second_id = submit_resp.deployment_id.unwrap().id;
                let switch_resp = grpc_client
                    .switch_deployment(SwitchDeploymentRequest {
                        deployment_id: Some(GrpcDeploymentId { id: second_id }),
                        runtime_config_check: RuntimeConfigCheck::Strict as i32,
                        hot_redeploy: true,
                    })
                    .await
                    .unwrap()
                    .into_inner();
                assert_eq!(switch_resp.outcome(), Outcome::SwitchOutcomeSwitched);
            }
            TestDeployClient::WebApi => {
                let id = server.webapi_submit_deployment(&new_deployment_toml).await;
                server.webapi_switch_hot_redeploy(id).await;
            }
        }
    }
}

/// Selects which protocol to use for execution replay in parametrized tests.
#[derive(Debug, Clone, Copy)]
enum TestExecutionClient {
    /// Replay via gRPC.
    Grpc,
    /// Replay via the Web API.
    WebApi,
}

fn grpc_result_to_json(value: grpc::grpc_gen::SupportedFunctionResult) -> serde_json::Value {
    match value.value {
        Some(grpc::grpc_gen::supported_function_result::Value::Ok(ok)) => {
            let return_value = ok.return_value.expect("ok return_value must exist");
            let ok = serde_json::from_slice::<serde_json::Value>(&return_value.value)
                .expect("server must send `return_value` as a valid JSON");
            json!({ "ok": ok })
        }
        Some(grpc::grpc_gen::supported_function_result::Value::Error(err)) => {
            let return_value = err.return_value.expect("err return_value must exist");
            let err = serde_json::from_slice::<serde_json::Value>(&return_value.value)
                .expect("server must send `return_value` as a valid JSON");
            json!({ "err": err })
        }
        Some(grpc::grpc_gen::supported_function_result::Value::ExecutionFailure(failure)) => {
            #[derive(serde::Serialize)]
            struct ExecutionFailure {
                kind: String,
                #[serde(skip_serializing_if = "Option::is_none")]
                reason: Option<String>,
                #[serde(skip_serializing_if = "Option::is_none")]
                detail: Option<String>,
            }
            #[derive(serde::Serialize)]
            struct ExecutionFailureWrapper {
                execution_failure: ExecutionFailure,
            }

            let kind = grpc::grpc_gen::ExecutionFailureKind::try_from(failure.kind)
                .expect("execution failure kind must be valid");
            let kind = match kind {
                grpc::grpc_gen::ExecutionFailureKind::Unspecified => {
                    panic!("execution failure kind must not be unspecified")
                }
                grpc::grpc_gen::ExecutionFailureKind::TimedOut => "timed_out",
                grpc::grpc_gen::ExecutionFailureKind::NondeterminismDetected => {
                    "nondeterminism_detected"
                }
                grpc::grpc_gen::ExecutionFailureKind::OutOfFuel => "out_of_fuel",
                grpc::grpc_gen::ExecutionFailureKind::Cancelled => "cancelled",
                grpc::grpc_gen::ExecutionFailureKind::Uncategorized => "uncategorized",
            }
            .to_string();
            serde_json::to_value(&ExecutionFailureWrapper {
                execution_failure: ExecutionFailure {
                    kind,
                    reason: failure.reason,
                    detail: failure.detail,
                },
            })
            .unwrap()
        }
        None => panic!("SupportedFunctionResult value must be set"),
    }
}

#[derive(Debug)]
struct ReplayCapturedWritesSummary {
    captured_writes_len: usize,
}

#[derive(Debug)]
struct AdvanceExecutionSummary {
    steps: usize,
    retval: serde_json::Value,
}

impl TestExecutionClient {
    async fn replay_captured_writes_summary(
        self,
        server: &TestServer,
        execution_id: &str,
    ) -> ReplayCapturedWritesSummary {
        match self {
            TestExecutionClient::WebApi => {
                let replay_resp = server.replay(execution_id).await;
                assert_eq!(replay_resp.status().as_u16(), 200);
                let replay_body: Value = replay_resp.json().await.unwrap();
                match replay_body["type"]
                    .as_str()
                    .expect("replay response type must be set")
                {
                    "advanceable" => ReplayCapturedWritesSummary {
                        captured_writes_len: replay_body["captured_writes"]
                            .as_array()
                            .expect("captured_writes must be an array")
                            .len(),
                    },
                    "finished" | "blocked" => ReplayCapturedWritesSummary {
                        captured_writes_len: 0,
                    },
                    other => panic!("unexpected replay response type {other}"),
                }
            }
            TestExecutionClient::Grpc => {
                let mut grpc_client =
                    ExecutionRepositoryClient::connect(format!("http://{}", server.api_addr()))
                        .await
                        .unwrap();
                let replay_resp = grpc_client
                    .replay_execution(ReplayExecutionRequest {
                        execution_id: Some(GrpcExecutionId {
                            id: execution_id.to_string(),
                        }),
                    })
                    .await
                    .unwrap()
                    .into_inner();
                match replay_resp.outcome.expect("replay outcome must be set") {
                    grpc::grpc_gen::replay_execution_response::Outcome::Advanceable(
                        advanceable,
                    ) => ReplayCapturedWritesSummary {
                        captured_writes_len: advanceable.captured_writes.len(),
                    },
                    grpc::grpc_gen::replay_execution_response::Outcome::Finished(_)
                    | grpc::grpc_gen::replay_execution_response::Outcome::Blocked(_) => {
                        ReplayCapturedWritesSummary {
                            captured_writes_len: 0,
                        }
                    }
                    grpc::grpc_gen::replay_execution_response::Outcome::ReplayFailed(failed) => {
                        panic!("unexpected replay failure: {}", failed.error)
                    }
                }
            }
        }
    }

    async fn step_execution_until_finished(
        self,
        server: &TestServer,
        ffqn: &str,
        params: Vec<Value>,
    ) -> AdvanceExecutionSummary {
        match self {
            TestExecutionClient::WebApi => {
                server
                    .step_execution_until_finished_webapi(ffqn, params)
                    .await
            }
            TestExecutionClient::Grpc => {
                server
                    .step_execution_until_finished_grpc(ffqn, params)
                    .await
            }
        }
    }

    async fn submit_paused(
        self,
        server: &TestServer,
        execution_id: &str,
        ffqn: &str,
        params: Vec<Value>,
    ) {
        match self {
            TestExecutionClient::WebApi => {
                let submit = server
                    .submit_paused_webapi(execution_id, ffqn, params)
                    .await;
                assert_eq!(submit.status().as_u16(), 201);
            }
            TestExecutionClient::Grpc => {
                let submit = server.submit_paused_grpc(execution_id, ffqn, params).await;
                assert_eq!(
                    submit.outcome,
                    grpc::grpc_gen::submit_response::Outcome::Created as i32
                );
            }
        }
    }
}

impl TestServer {
    async fn submit_paused_grpc(
        &self,
        execution_id: &str,
        ffqn: &str,
        params: Vec<Value>,
    ) -> grpc::grpc_gen::SubmitResponse {
        let mut grpc_client =
            ExecutionRepositoryClient::connect(format!("http://{}", self.api_addr()))
                .await
                .unwrap();
        grpc_client
            .submit(SubmitRequest {
                execution_id: Some(GrpcExecutionId {
                    id: execution_id.to_string(),
                }),
                function_name: Some(ffqn.parse::<FunctionFqn>().unwrap().into()),
                params: Some(
                    grpc::grpc_mapping::to_any(params, format!("urn:obelisk:json:params:{ffqn}"))
                        .unwrap(),
                ),
                paused: true,
            })
            .await
            .unwrap()
            .into_inner()
    }

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

    async fn get_status_summary_grpc(
        &self,
        execution_id: &str,
    ) -> grpc::grpc_gen::ExecutionSummary {
        let mut grpc_client =
            ExecutionRepositoryClient::connect(format!("http://{}", self.api_addr()))
                .await
                .unwrap();
        let mut stream = grpc_client
            .get_status(GetStatusRequest {
                execution_id: Some(GrpcExecutionId {
                    id: execution_id.to_string(),
                }),
                follow: false,
                send_finished_status: false,
            })
            .await
            .unwrap()
            .into_inner();

        let mut summary = None;
        while let Some(message) = stream.next().await {
            let message = message.unwrap();
            match message.message {
                Some(grpc::grpc_gen::get_status_response::Message::Summary(found)) => {
                    summary = Some(found);
                }
                Some(grpc::grpc_gen::get_status_response::Message::FinishedStatus(_)) => {
                    panic!("send_finished_status=false should not emit finished_status")
                }
                Some(grpc::grpc_gen::get_status_response::Message::CurrentStatus(_)) => {
                    panic!("follow=false should not emit current_status")
                }
                None => panic!("get_status message must be set"),
            }
        }
        summary.expect("summary must be present")
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
                            params: Params::empty(),
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

    async fn step_execution_until_finished_grpc(
        &self,
        ffqn: &str,
        params: Vec<Value>,
    ) -> AdvanceExecutionSummary {
        let exec_id = self.generate_execution_id().await;
        let submit = self.submit_paused_grpc(&exec_id, ffqn, params).await;
        assert_eq!(
            submit.outcome(),
            grpc::grpc_gen::submit_response::Outcome::Created
        );

        let initial_summary = self.get_status_summary_grpc(&exec_id).await;
        assert!(matches!(
            initial_summary
                .current_status
                .as_ref()
                .and_then(|status| status.status.as_ref()),
            Some(grpc::grpc_gen::execution_status::Status::Paused(_))
        ));

        let mut grpc_client =
            ExecutionRepositoryClient::connect(format!("http://{}", self.api_addr()))
                .await
                .unwrap();

        let mut steps = 0;
        loop {
            let replay = grpc_client
                .replay_execution(ReplayExecutionRequest {
                    execution_id: Some(GrpcExecutionId {
                        id: exec_id.clone(),
                    }),
                })
                .await
                .unwrap()
                .into_inner();
            let captured_writes = match replay.outcome.expect("replay outcome must be set") {
                grpc::grpc_gen::replay_execution_response::Outcome::Advanceable(advanceable) => {
                    advanceable.captured_writes
                }
                grpc::grpc_gen::replay_execution_response::Outcome::Finished(finished) => {
                    let value = finished.result.expect("finished result must be set");
                    return AdvanceExecutionSummary {
                        steps,
                        retval: grpc_result_to_json(value),
                    };
                }
                grpc::grpc_gen::replay_execution_response::Outcome::Blocked(_) => {
                    unreachable!("blocked state is not created by any test")
                }
                grpc::grpc_gen::replay_execution_response::Outcome::ReplayFailed(failed) => {
                    failed.captured_writes
                }
            };

            steps += 1;
            let advance = grpc_client
                .advance_execution(AdvanceExecutionRequest {
                    execution_id: Some(GrpcExecutionId {
                        id: exec_id.clone(),
                    }),
                    captured_writes,
                })
                .await
                .unwrap()
                .into_inner();
            match advance.result.expect("advance result must be set") {
                grpc::grpc_gen::advance_execution_response::Result::Success(success) => {
                    if let Some(value) = success.finished {
                        return AdvanceExecutionSummary {
                            steps,
                            retval: grpc_result_to_json(value),
                        };
                    }
                }
                grpc::grpc_gen::advance_execution_response::Result::Error(error) => {
                    match error.error.expect("advance error must be set") {
                        grpc::grpc_gen::advance_execution_response::error::Error::VersionMismatch(_) => {
                            panic!("advance returned version mismatch on step {steps}")
                        }
                        grpc::grpc_gen::advance_execution_response::error::Error::ReplayMismatch(_) => {
                            panic!("advance returned replay mismatch on step {steps}")
                        }
                        grpc::grpc_gen::advance_execution_response::error::Error::TransientError(err) => {
                            panic!("advance returned replay error on step {steps}: {}", err.message)
                        }
                    }
                }
            }

            let summary = self.get_status_summary_grpc(&exec_id).await;
            assert!(
                !Self::is_finished(&summary),
                "finished executions should return finished from AdvanceExecution"
            );
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
            let captured_writes = match replay_body {
                ReplayResponseSer::Advanceable { captured_writes }
                | ReplayResponseSer::ReplayFailed {
                    captured_writes, ..
                } => captured_writes,
                ReplayResponseSer::Finished { retval: _ } => {
                    panic!("should have been returned as `advance` response first");
                }
                ReplayResponseSer::Blocked => {
                    unreachable!("blocked state is not created by any test")
                }
            };
            assert!(!captured_writes.is_empty());
            steps += 1;
            let advance = self
                .client
                .put(format!("{}/v1/executions/{exec_id}/advance", self.base_url))
                .header("Accept", "application/json")
                .json(&json!({ "captured_writes": captured_writes }))
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
                    return AdvanceExecutionSummary { steps, retval };
                }
                AdvanceResponseDeser::InProgress => {
                    // continue the loop
                }
            }
        }
    }

    fn is_finished(summary: &grpc::grpc_gen::ExecutionSummary) -> bool {
        matches!(
            summary
                .current_status
                .as_ref()
                .and_then(|status| status.status.as_ref()),
            Some(grpc::grpc_gen::execution_status::Status::Finished(_))
        )
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
async fn list_components_grpc_by_explicit_deployment_id() {
    let server = TestServer::start(test_addr!(40_102)).await;
    const NEW_STUB_NAME: &str = "explicit_deployment_stub_grpc";
    const NEW_STUB_FFQN: &str = "testing:integration/stubs.explicit-deployment-grpc";

    let second_deployment_id = server
        .submit_modified_deployment(|doc| {
            append_inline_stub(doc, NEW_STUB_NAME, NEW_STUB_FFQN);
        })
        .await;

    let current_components = server.grpc_list_components(None).await;
    assert!(
        !current_components.components.iter().any(|component| {
            component.exports.iter().any(|export| {
                export.function_name.as_ref().is_some_and(|function_name| {
                    function_name.function_name == "explicit-deployment-grpc"
                })
            })
        }),
        "inactive deployment component must not appear without explicit deployment_id"
    );

    let explicit_components = server
        .grpc_list_components(Some(second_deployment_id))
        .await;
    assert!(
        explicit_components.components.iter().any(|component| {
            component
                .component_id
                .as_ref()
                .is_some_and(|component_id| component_id.name == NEW_STUB_NAME)
        }),
        "explicit deployment_id must return the submitted deployment's components"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn list_components_grpc_filters_with_explicit_deployment_id() {
    let server = TestServer::start(test_addr!(40_103)).await;
    const NEW_STUB_NAME: &str = "explicit_deployment_stub_grpc_filters";
    const NEW_STUB_FFQN: &str = "testing:integration/stubs.explicit-deployment-grpc-filters";

    let second_deployment_id = server
        .submit_modified_deployment(|doc| {
            append_inline_stub(doc, NEW_STUB_NAME, NEW_STUB_FFQN);
        })
        .await;

    let explicit_components = server
        .grpc_list_components(Some(second_deployment_id))
        .await;
    let target = explicit_components
        .components
        .iter()
        .find(|component| {
            component
                .component_id
                .as_ref()
                .is_some_and(|component_id| component_id.name == NEW_STUB_NAME)
        })
        .unwrap();
    let digest = target
        .component_id
        .as_ref()
        .unwrap()
        .digest
        .as_ref()
        .unwrap()
        .digest
        .clone();

    let mut fn_client = FunctionRepositoryClient::connect(format!("http://{}", server.api_addr()))
        .await
        .unwrap();
    let filtered = fn_client
        .list_components(ListComponentsRequest {
            function_name: Some(grpc::grpc_gen::FunctionName::from(
                &NEW_STUB_FFQN.parse::<FunctionFqn>().unwrap(),
            )),
            component_digest: Some(grpc::grpc_gen::ContentDigest { digest }),
            extensions: false,
            deployment_id: Some(GrpcDeploymentId {
                id: second_deployment_id.to_string(),
            }),
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(1, filtered.components.len());
    assert_eq!(
        NEW_STUB_NAME,
        filtered.components[0].component_id.as_ref().unwrap().name
    );

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
    let body1: Value = resp1.json().await.unwrap();
    let returned1: DeploymentId = body1["deployment_id"].as_str().unwrap().parse().unwrap();
    assert_eq!(deployment_id, returned1, "explicit ID must be honored");

    // Resubmitting the identical manifest under the same ID is an idempotent no-op.
    let resp2 = server
        .webapi_submit_deployment_with_id(&deployment_toml, deployment_id)
        .await;
    assert!(
        resp2.status().is_success(),
        "idempotent resubmit failed: {}",
        resp2.status()
    );
    let body2: Value = resp2.json().await.unwrap();
    let returned2: DeploymentId = body2["deployment_id"].as_str().unwrap().parse().unwrap();
    assert_eq!(
        deployment_id, returned2,
        "no-op resubmit must return same ID"
    );

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
        reqwest::StatusCode::BAD_REQUEST,
        "different config under same ID must be rejected as a digest conflict"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn submit_deployment_package_validates_before_persisting_grpc() {
    use prost::Message as _;

    let server = TestServer::start(test_addr!(62)).await;

    let dir = tempfile::tempdir().unwrap();
    tokio::fs::write(
        dir.path().join("deferred.js"),
        "export function run() { return 'ok'; }",
    )
    .await
    .unwrap();
    let deployment_toml_path = dir.path().join("deployment.toml");
    tokio::fs::write(
        &deployment_toml_path,
        r#"
[[activity_js]]
name = "deferred"
location = "deferred.js"
ffqn = "testing:integration/deferred.run"
"#,
    )
    .await
    .unwrap();
    let prepared =
        crate::config::manifest::prepare_deployment_manifest_from_disk(&deployment_toml_path)
            .await
            .unwrap();
    let expected_digest = prepared.files[0].digest.to_string();
    let deployment_id = DeploymentId::generate();
    let grpc_id = GrpcDeploymentId {
        id: deployment_id.to_string(),
    };

    let mut grpc_client =
        DeploymentRepositoryClient::connect(format!("http://{}", server.api_addr()))
            .await
            .unwrap()
            .max_encoding_message_size(crate::MAX_GRPC_MESSAGE_SIZE)
            .max_decoding_message_size(crate::MAX_GRPC_MESSAGE_SIZE);
    let submit = |files, id| {
        let mut client = grpc_client.clone();
        let toml = prepared.deployment_toml.clone();
        async move {
            client
                .submit_deployment(SubmitDeploymentRequest {
                    deployment_toml: toml,
                    created_by: Some("test".to_string()),
                    runtime_config_check: RuntimeConfigCheck::Strict as i32,
                    description: None,
                    deployment_id: Some(id),
                    files,
                })
                .await
        }
    };

    // Preflight with no blobs reports the missing file and stores nothing.
    let status = submit(Vec::new(), grpc_id.clone())
        .await
        .expect_err("preflight must report the missing file");
    let detail = grpc::grpc_gen::SubmitDeploymentErrorDetail::decode(status.details())
        .expect("must carry SubmitDeploymentErrorDetail");
    assert_eq!(detail.missing_files.len(), 1);
    assert_eq!(
        detail.missing_files[0].digest.as_deref(),
        Some(expected_digest.as_str())
    );
    assert_eq!(detail.missing_files[0].section, "activity_js");
    grpc_client
        .clone()
        .get_deployment(GetDeploymentRequest {
            deployment_id: Some(grpc_id.clone()),
        })
        .await
        .expect_err("failed preflight must not persist a deployment");

    // An attached blob not referenced by the manifest is rejected as unexpected.
    let status = submit(
        vec![grpc::grpc_gen::DeploymentFileContent {
            path: "deferred.js".to_string(),
            digest: None,
            content: b"unexpected".to_vec(),
        }],
        grpc_id.clone(),
    )
    .await
    .expect_err("unexpected blob must be rejected");
    let detail = grpc::grpc_gen::SubmitDeploymentErrorDetail::decode(status.details()).unwrap();
    assert_eq!(detail.unexpected_files.len(), 1);

    // A client-supplied digest that disagrees with the bytes is rejected.
    let status = submit(
        vec![grpc::grpc_gen::DeploymentFileContent {
            path: "deferred.js".to_string(),
            digest: Some(
                "sha256:0000000000000000000000000000000000000000000000000000000000000000"
                    .to_string(),
            ),
            content: prepared.files[0].bytes.clone(),
        }],
        grpc_id.clone(),
    )
    .await
    .expect_err("digest mismatch must be rejected");
    let detail = grpc::grpc_gen::SubmitDeploymentErrorDetail::decode(status.details()).unwrap();
    assert_eq!(detail.digest_mismatches.len(), 1);

    // Retry with the correct blob persists the complete deployment.
    let resp = submit(
        vec![grpc::grpc_gen::DeploymentFileContent {
            path: "deferred.js".to_string(),
            digest: Some(expected_digest.clone()),
            content: prepared.files[0].bytes.clone(),
        }],
        grpc_id.clone(),
    )
    .await
    .expect("submit with correct blob must succeed")
    .into_inner();
    assert_eq!(
        resp.deployment_id.expect("deployment_id").id,
        deployment_id.to_string()
    );

    // The stored deployment is complete and now exists.
    grpc_client
        .get_deployment(GetDeploymentRequest {
            deployment_id: Some(grpc_id),
        })
        .await
        .expect("stored deployment must exist");

    server.shutdown().await;
}

/// Phase 5: `RuntimeConfigCheck` governs whether missing env vars / secrets fail
/// verification. Submit is strict by default and tolerant under `ALLOW_MISSING`; a
/// hot redeploy is always strict and rejects `ALLOW_MISSING` outright.
#[tokio::test]
async fn submit_runtime_config_check_grpc() {
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

    let grpc_client = DeploymentRepositoryClient::connect(format!("http://{}", server.api_addr()))
        .await
        .unwrap()
        .max_encoding_message_size(crate::MAX_GRPC_MESSAGE_SIZE)
        .max_decoding_message_size(crate::MAX_GRPC_MESSAGE_SIZE);

    let submit = |check: RuntimeConfigCheck, id: Option<GrpcDeploymentId>| {
        let mut client = grpc_client.clone();
        async move {
            client
                .submit_deployment(SubmitDeploymentRequest {
                    deployment_toml: deployment_toml.to_string(),
                    created_by: Some("test".to_string()),
                    runtime_config_check: check as i32,
                    description: None,
                    deployment_id: id,
                    files: Vec::new(),
                })
                .await
        }
    };

    // Strict submit fails on the missing env var and stores nothing.
    let status = submit(RuntimeConfigCheck::Strict, None)
        .await
        .expect_err("strict submit must reject a missing env var");
    assert!(
        status
            .message()
            .contains("OBELISK_PHASE5_DEFINITELY_MISSING_VAR"),
        "error must name the missing env var, got: {}",
        status.message()
    );

    // ALLOW_MISSING submit tolerates the missing env var and persists.
    let stored_id = submit(RuntimeConfigCheck::AllowMissing, None)
        .await
        .expect("allow-missing submit must persist")
        .into_inner()
        .deployment_id
        .expect("deployment_id")
        .id;
    let stored_grpc_id = GrpcDeploymentId { id: stored_id };

    // A hot redeploy with ALLOW_MISSING is rejected outright.
    let status = grpc_client
        .clone()
        .switch_deployment(SwitchDeploymentRequest {
            deployment_id: Some(stored_grpc_id.clone()),
            runtime_config_check: RuntimeConfigCheck::AllowMissing as i32,
            hot_redeploy: true,
        })
        .await
        .expect_err("hot redeploy must reject allow-missing-runtime-config");
    assert_eq!(
        "argument `runtime_config_check = RUNTIME_CONFIG_CHECK_ALLOW_MISSING` cannot be used with `hot_redeploy = true`",
        status.message()
    );

    // A strict hot redeploy of the same deployment fails because the env var is missing.
    let status = grpc_client
        .clone()
        .switch_deployment(SwitchDeploymentRequest {
            deployment_id: Some(stored_grpc_id),
            runtime_config_check: RuntimeConfigCheck::Strict as i32,
            hot_redeploy: true,
        })
        .await
        .expect_err("strict hot redeploy must fail on the missing env var");
    assert!(
        status
            .message()
            .contains("OBELISK_PHASE5_DEFINITELY_MISSING_VAR"),
        "error must name the missing env var, got: {}",
        status.message()
    );

    server.shutdown().await;
}

/// Phase 6: the REST `POST /v1/deployments` accepts a `multipart/form-data` package
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
        crate::config::manifest::prepare_deployment_manifest_from_disk(&deployment_toml_path)
            .await
            .unwrap();
    let toml = prepared.deployment_toml.clone();
    let expected_digest = prepared.files[0].digest.to_string();
    let content = prepared.files[0].bytes.clone();
    let url = format!("{}/v1/deployments", server.base_url);

    // Preflight with no file parts: incomplete package, stored nothing, lists the missing file.
    let resp = server
        .client
        .post(&url)
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

    // A blob whose supplied digest disagrees with its bytes is reported as a mismatch.
    let mismatch_part = reqwest::multipart::Part::bytes(content.clone())
        .file_name("pkg.js")
        .mime_str("application/octet-stream")
        .unwrap();
    let resp = server
        .client
        .post(&url)
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
        .post(&url)
        .header("Accept", "application/json")
        .multipart(
            reqwest::multipart::Form::new()
                .text("deployment_toml", toml)
                .part(expected_digest.clone(), part),
        )
        .send()
        .await
        .expect("multipart retry failed");
    assert_eq!(resp.status().as_u16(), 200, "complete package must succeed");
    let body: Value = resp.json().await.unwrap();
    let deployment_id = body["deployment_id"].as_str().expect("deployment_id");

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

    server.shutdown().await;
}

/// Phase 8: a submit that writes file blobs and then fails verification leaves orphan
/// blobs in the CAS. `GcOrphanFiles` deletes blobs not referenced by any stored
/// deployment while keeping those a valid deployment still references.
#[tokio::test]
async fn gc_orphan_files_grpc() {
    let server = TestServer::start(test_addr!(81)).await;

    let grpc_client = DeploymentRepositoryClient::connect(format!("http://{}", server.api_addr()))
        .await
        .unwrap()
        .max_encoding_message_size(crate::MAX_GRPC_MESSAGE_SIZE)
        .max_decoding_message_size(crate::MAX_GRPC_MESSAGE_SIZE);

    // Prepare a deployment-owned JS file (digest-enriched) from disk. `section` selects
    // `activity_js` or `workflow_js` (a JS workflow validates its source at link time, so
    // a syntax error there fails submit verification).
    async fn prepare(
        section: &str,
        source: &str,
    ) -> (
        tempfile::TempDir,
        crate::config::manifest::PreparedDeploymentManifest,
    ) {
        let dir = tempfile::tempdir().unwrap();
        tokio::fs::write(dir.path().join("a.js"), source)
            .await
            .unwrap();
        let toml_path = dir.path().join("deployment.toml");
        tokio::fs::write(
            &toml_path,
            format!(
                r#"
[[{section}]]
name = "a"
location = "a.js"
ffqn = "testing:integration/a.run"
"#
            ),
        )
        .await
        .unwrap();
        let prepared = crate::config::manifest::prepare_deployment_manifest_from_disk(&toml_path)
            .await
            .unwrap();
        (dir, prepared)
    }

    let submit_request =
        |prepared: &crate::config::manifest::PreparedDeploymentManifest| SubmitDeploymentRequest {
            deployment_toml: prepared.deployment_toml.clone(),
            created_by: Some("test".to_string()),
            runtime_config_check: RuntimeConfigCheck::Strict as i32,
            description: None,
            deployment_id: Some(GrpcDeploymentId {
                id: DeploymentId::generate().to_string(),
            }),
            files: vec![grpc::grpc_gen::DeploymentFileContent {
                path: "a.js".to_string(),
                digest: Some(prepared.files[0].digest.to_string()),
                content: prepared.files[0].bytes.clone(),
            }],
        };

    // A valid deployment: its blob is referenced and must survive GC.
    let (_good_dir, good) = prepare("activity_js", "export function run() { return 'ok'; }").await;
    let good_digest = good.files[0].digest.to_string();
    grpc_client
        .clone()
        .submit_deployment(submit_request(&good))
        .await
        .expect("valid deployment must persist");

    // An invalid deployment: the blob is written, then verification fails, so no
    // deployment row references it. It becomes an orphan. A JS workflow validates its
    // source at link time, so a syntax error fails submit.
    let (_bad_dir, bad) = prepare("workflow_js", "this is @@@ not valid javascript {{{").await;
    let bad_digest = bad.files[0].digest.to_string();
    grpc_client
        .clone()
        .submit_deployment(submit_request(&bad))
        .await
        .expect_err("invalid deployment must fail verification");

    // Before GC: both blobs are present in the CAS.
    grpc_client
        .clone()
        .get_file(GetFileRequest {
            digest: good_digest.clone(),
        })
        .await
        .expect("referenced blob present before gc");
    grpc_client
        .clone()
        .get_file(GetFileRequest {
            digest: bad_digest.clone(),
        })
        .await
        .expect("orphan blob present before gc");

    // GC deletes exactly the orphan.
    let deleted = grpc_client
        .clone()
        .gc_orphan_files(GcOrphanFilesRequest {})
        .await
        .unwrap()
        .into_inner()
        .deleted_count;
    assert_eq!(deleted, 1, "exactly one orphan blob must be deleted");

    // After GC: the orphan is gone, the referenced blob remains.
    grpc_client
        .clone()
        .get_file(GetFileRequest {
            digest: good_digest,
        })
        .await
        .expect("referenced blob must survive gc");
    let status = grpc_client
        .clone()
        .get_file(GetFileRequest { digest: bad_digest })
        .await
        .expect_err("orphan blob must be gone after gc");
    assert_eq!(status.code(), tonic::Code::NotFound);

    // A second GC is a no-op.
    let deleted = grpc_client
        .clone()
        .gc_orphan_files(GcOrphanFilesRequest {})
        .await
        .unwrap()
        .into_inner()
        .deleted_count;
    assert_eq!(deleted, 0, "second gc must delete nothing");

    server.shutdown().await;
}

// ---- Activity: submit + result ----

#[tokio::test]
async fn submit_activity_and_get_result() {
    let server = TestServer::start(test_addr!(4)).await;

    let resp = server
        .submit_follow("testing:integration/activity.add", vec![json!(3), json!(5)])
        .await;
    assert_eq!(resp.status().as_u16(), 201);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body, json!({ "ok": 8 }));
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

#[tokio::test]
async fn cancel_execution_grpc_routes_activities_and_cancellable_workflows() {
    let server = TestServer::start(test_addr!(83)).await;
    let mut grpc_client =
        ExecutionRepositoryClient::connect(format!("http://{}", server.api_addr()))
            .await
            .unwrap();

    let activity_id = server.generate_execution_id().await;
    server
        .submit_paused_grpc(
            &activity_id,
            "testing:integration/activity.add",
            vec![json!(3), json!(5)],
        )
        .await;
    let resp = grpc_client
        .cancel_execution(CancelExecutionRequest {
            execution_id: Some(GrpcExecutionId {
                id: activity_id.clone(),
            }),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        resp.outcome(),
        CancelExecutionOutcome::CancellationRequested
    );
    // Cancelling a paused activity is async: the driver finalizes it to
    // Finished(Cancelled) on a later tick, so poll rather than asserting immediately.
    let mut activity_finished = false;
    for _ in 0..100 {
        let summary = server.get_status_summary_grpc(&activity_id).await;
        if matches!(
            summary
                .current_status
                .as_ref()
                .and_then(|status| status.status.as_ref()),
            Some(grpc::grpc_gen::execution_status::Status::Finished(_))
        ) {
            activity_finished = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(activity_finished, "cancelled paused activity must finish");

    let cancellable_workflow_id = server
        .seed_cancellable_parent_blocked_on_uncancellable_child()
        .await
        .to_string();
    let resp = grpc_client
        .cancel_execution(CancelExecutionRequest {
            execution_id: Some(GrpcExecutionId {
                id: cancellable_workflow_id.clone(),
            }),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        resp.outcome(),
        CancelExecutionOutcome::CancellationRequested
    );
    let summary = server
        .get_status_summary_grpc(&cancellable_workflow_id)
        .await;
    assert!(matches!(
        summary
            .current_status
            .as_ref()
            .and_then(|status| status.status.as_ref()),
        Some(grpc::grpc_gen::execution_status::Status::Cancelling(_))
    ));
    let resp = grpc_client
        .cancel_execution(CancelExecutionRequest {
            execution_id: Some(GrpcExecutionId {
                id: cancellable_workflow_id.clone(),
            }),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.outcome(), CancelExecutionOutcome::AlreadyCancelling);

    let workflow_id = server.generate_execution_id().await;
    server
        .submit_paused_grpc(
            &workflow_id,
            "testing:integration/workflow-add.add-workflow",
            vec![json!(1), json!(2)],
        )
        .await;
    let status = grpc_client
        .cancel_execution(CancelExecutionRequest {
            execution_id: Some(GrpcExecutionId { id: workflow_id }),
        })
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    assert!(
        status
            .message()
            .contains("cannot cancel, execution is not a cancellable workflow")
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
    let mut activity_finished = false;
    for _ in 0..100 {
        let summary = server.get_status_summary_grpc(&activity_id).await;
        if matches!(
            summary
                .current_status
                .as_ref()
                .and_then(|status| status.status.as_ref()),
            Some(grpc::grpc_gen::execution_status::Status::Finished(_))
        ) {
            activity_finished = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(activity_finished, "cancelled paused activity must finish");

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
    let summary = server
        .get_status_summary_grpc(&cancellable_workflow_id)
        .await;
    assert!(matches!(
        summary
            .current_status
            .as_ref()
            .and_then(|status| status.status.as_ref()),
        Some(grpc::grpc_gen::execution_status::Status::Cancelling(_))
    ));
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
    use grpc::grpc_gen::execution_status::Status;
    const FFQN: &str = "testing:integration/workflow-sleep.sleep-cancellable";
    let server = TestServer::start(test_addr!(85)).await;
    let mut grpc_client =
        ExecutionRepositoryClient::connect(format!("http://{}", server.api_addr()))
            .await
            .unwrap();

    // Submit running (not paused); the workflow blocks on a 100s durable sleep.
    let exec_id = server.generate_execution_id().await;
    grpc_client
        .submit(SubmitRequest {
            execution_id: Some(GrpcExecutionId {
                id: exec_id.clone(),
            }),
            function_name: Some(FFQN.parse::<FunctionFqn>().unwrap().into()),
            params: Some(
                grpc::grpc_mapping::to_any(
                    Vec::<Value>::new(),
                    format!("urn:obelisk:json:params:{FFQN}"),
                )
                .unwrap(),
            ),
            paused: false,
        })
        .await
        .unwrap();

    // Wait until it has run and created the delay (blocked, or kept warm-locked).
    let mut started = false;
    for _ in 0..100 {
        let status = server.get_status_summary_grpc(&exec_id).await;
        if matches!(
            status
                .current_status
                .as_ref()
                .and_then(|status| status.status.as_ref()),
            Some(Status::BlockedByJoinSet(_) | Status::Locked(_))
        ) {
            started = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(started, "workflow never started/blocked on the sleep");

    let resp = grpc_client
        .cancel_execution(CancelExecutionRequest {
            execution_id: Some(GrpcExecutionId {
                id: exec_id.clone(),
            }),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        resp.outcome(),
        CancelExecutionOutcome::CancellationRequested
    );

    // The driver drives it to Finished(Cancelled) without the 100s sleep ever expiring.
    let mut finished_kind = None;
    for _ in 0..100 {
        let status = server.get_status_summary_grpc(&exec_id).await;
        if let Some(Status::Finished(finished)) = status
            .current_status
            .as_ref()
            .and_then(|status| status.status.as_ref())
        {
            finished_kind = Some(
                finished
                    .result_kind
                    .as_ref()
                    .and_then(|rk| rk.value)
                    .expect("finished must carry a result kind"),
            );
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        finished_kind,
        Some(grpc::grpc_gen::result_kind::Value::ExecutionFailureKind(
            grpc::grpc_gen::ExecutionFailureKind::Cancelled as i32
        )),
        "workflow must finish as Cancelled"
    );

    server.shutdown().await;
}

// ---- Workflow: replaying a paused workflow should return preview events ----

#[tokio::test]
async fn replaying_paused_workflow_should_return_preview_events_grpc() {
    replaying_paused_workflow_should_return_preview_events(
        TestExecutionClient::Grpc,
        test_addr!(63),
    )
    .await;
}

#[tokio::test]
async fn replaying_paused_workflow_should_return_preview_events_webapi() {
    replaying_paused_workflow_should_return_preview_events(
        TestExecutionClient::WebApi,
        test_addr!(66),
    )
    .await;
}

async fn replaying_paused_workflow_should_return_preview_events(
    client: TestExecutionClient,
    addr: String,
) {
    let server = TestServer::start(addr).await;

    let exec_id = server.generate_execution_id().await;

    client
        .submit_paused(
            &server,
            &exec_id,
            "testing:integration/workflow-add-via-activity.add-via-activity",
            vec![json!(3), json!(4)],
        )
        .await;

    let replay = client
        .replay_captured_writes_summary(&server, &exec_id)
        .await;
    assert!(
        replay.captured_writes_len > 0,
        "captured_writes must not be empty for a paused workflow: {client:?}"
    );

    server.shutdown().await;
}

async fn replay_and_advance_paused_js_workflow_until_finished(
    client: TestExecutionClient,
    addr: String,
) {
    let server = TestServer::start(addr).await;
    let stepped = client
        .step_execution_until_finished(
            &server,
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
async fn replay_and_advance_paused_js_workflow_until_finished_grpc() {
    replay_and_advance_paused_js_workflow_until_finished(TestExecutionClient::Grpc, test_addr!(64))
        .await;
}

#[tokio::test]
async fn replay_and_advance_paused_js_workflow_until_finished_webapi() {
    replay_and_advance_paused_js_workflow_until_finished(
        TestExecutionClient::WebApi,
        test_addr!(65),
    )
    .await;
}

// ---- Workflow: replay failed (type mismatch) with advance --force ----

const REPLAY_FAILED_FFQN: &str = "testing:integration/workflow-return-wrong-type.return-wrong-type";

async fn replay_failed_js_workflow_then_advance(client: TestExecutionClient, addr: String) {
    let server = TestServer::start(addr).await;
    let stepped = client
        .step_execution_until_finished(&server, REPLAY_FAILED_FFQN, vec![])
        .await;
    assert_eq!(stepped.steps, 1);
    assert_eq!(
        json!(
            {
                "execution_failure":{
                    "kind":"uncategorized",
                    "reason":"value does not type check - failed to type check the ok variant value `\"not-a-number\"` as type u32 - invalid type: string \"not-a-number\", expected value matching \"u32\" at line 1 column 14"
                }
            }
        ),
        stepped.retval
    );
    server.shutdown().await;
}

#[tokio::test]
async fn replay_failed_js_workflow_then_advance_webapi() {
    replay_failed_js_workflow_then_advance(TestExecutionClient::WebApi, test_addr!(67)).await;
}

#[tokio::test]
async fn replay_failed_js_workflow_then_advance_grpc() {
    replay_failed_js_workflow_then_advance(TestExecutionClient::Grpc, test_addr!(68)).await;
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
    assert_eq!(body, json!({ "ok": 7 }));

    let events = server.get_events(&exec_id).await;
    let events = sanitize_json(&events);
    insta::assert_json_snapshot!("workflow_call_activity_events", events);
    server.shutdown().await;
}

// ---- Workflow: ES module import calling activity ----

#[tokio::test]
async fn submit_workflow_with_import_call() {
    let server = TestServer::start(test_addr!(69)).await;
    let exec_id = server.generate_execution_id().await;

    let resp = server
        .submit_follow_with_id(
            &exec_id,
            "testing:integration/workflow-import-call-activity.call-activity",
            vec![json!(3), json!(4)],
        )
        .await;
    assert_eq!(resp.status().as_u16(), 201);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body, json!({ "ok": 7 }));

    server.shutdown().await;
}

// ---- Workflow: ES module namespace import (import *) calling activity ----

#[tokio::test]
async fn submit_workflow_with_import_star_call() {
    let server = TestServer::start(test_addr!(70)).await;
    let exec_id = server.generate_execution_id().await;

    let resp = server
        .submit_follow_with_id(
            &exec_id,
            "testing:integration/workflow-import-star-call-activity.call-activity",
            vec![json!(3), json!(4)],
        )
        .await;
    assert_eq!(resp.status().as_u16(), 201);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body, json!({ "ok": 7 }));

    server.shutdown().await;
}

// ---- Workflow: ES module schedule import ----

#[tokio::test]
async fn submit_workflow_with_import_schedule() {
    let server = TestServer::start(test_addr!(71)).await;
    let exec_id = server.generate_execution_id().await;

    let resp = server
        .submit_follow_with_id(
            &exec_id,
            "testing:integration/workflow-import-schedule-activity.schedule-activity",
            vec![json!(3), json!(4)],
        )
        .await;
    assert_eq!(resp.status().as_u16(), 201);
    let body: Value = resp.json().await.unwrap();
    // schedule returns Ok(execution_id_string)
    assert!(
        body["ok"].is_string(),
        "expected ok to be an execution ID string, got: {body}"
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

// ---- Workflow: ES module ext import (submit/awaitNext) ----

#[tokio::test]
async fn submit_workflow_with_import_ext() {
    let server = TestServer::start(test_addr!(74)).await;
    let exec_id = server.generate_execution_id().await;

    let resp = server
        .submit_follow_with_id(
            &exec_id,
            "testing:integration/workflow-import-ext-activity.add-via-activity",
            vec![json!(7), json!(8)],
        )
        .await;
    assert_eq!(resp.status().as_u16(), 201);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body, json!({ "ok": 15 }));

    server.shutdown().await;
}

// ---- Workflow: ES module stub import ----

#[tokio::test]
async fn submit_workflow_with_import_stub() {
    let server = TestServer::start(test_addr!(75)).await;
    let resp = server
        .submit_follow(
            "testing:integration/workflow-import-stub-activity.call-stub",
            vec![json!(42u64)],
        )
        .await;
    assert_eq!(resp.status().as_u16(), 201);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body, json!({ "ok": "stub-ok" }));

    server.shutdown().await;
}

#[tokio::test]
async fn submit_workflow_with_join_next_try_semantics() {
    let server = TestServer::start(test_addr!(77)).await;
    let resp = server
        .submit_follow(
            "testing:integration/workflow-join-next-try-semantics.join-next-try-semantics",
            vec![json!(42u64)],
        )
        .await;
    assert_eq!(resp.status().as_u16(), 201);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body, json!({ "ok": "stub-ok" }));

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
    // Verify executionIdCurrent is returned via x-execution-id header
    let exec_id = resp
        .headers()
        .get("x-execution-id")
        .expect("x-execution-id header must be present")
        .to_str()
        .unwrap();
    assert!(
        exec_id.starts_with("E_"),
        "execution ID must have E_ prefix, got: {exec_id}"
    );
    let body = resp.text().await.unwrap();
    assert_eq!(body, "Hello from JS webhook!");
    server.shutdown().await;
}

/// A JS webhook's `getStatus` reports the first-class `cancelling` state
/// (via `get-status-v2`).
///
/// The seed needs an *uncancellable* child, not just one execution: the running
/// server's cancellation driver finishes any `cancelling` execution once all its
/// join-set members have responded, so a lone one would be `Finished(Cancelled)`
/// before the webhook reads it. An uncancellable child that never responds is an
/// await barrier the driver can't clear, holding the parent in `cancelling`.
#[tokio::test]
async fn webhook_js_get_status_cancelling() {
    use concepts::prefixed_ulid::DEPLOYMENT_ID_DUMMY;
    use concepts::storage::{
        AppendRequest, CreateRequest, ExecutionRequest, HistoryEvent, JoinSetRequest,
    };
    use concepts::{
        ComponentId, ExecutionId, ExecutionMetadata, JoinSetId, JoinSetKind, Params, StrVariant,
    };

    // Uncancellable child (no `-cancellable` suffix) → permanent await barrier.
    const PARENT_FFQN: FunctionFqn =
        FunctionFqn::new_static("testing:cancel/ifc", "parent-cancellable");
    const CHILD_FFQN: FunctionFqn = FunctionFqn::new_static("testing:cancel/ifc", "child");

    let server = TestServer::start(test_addr!(86)).await;

    // Seed a cancelling parent with an unfinished uncancellable child directly in
    // the server's sqlite DB.
    let parent_id = {
        let pool = SqlitePool::new(&server.sqlite_file, SqliteConfig::default())
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
                            params: Params::empty(),
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
        conn.cancel_workflow_with_retries(&parent_id, now)
            .await
            .unwrap();
        pool.close().await;
        parent_id
    };

    let resp = server
        .client
        .get(format!("{}/get-status", server.webhook_base_url))
        .header("x-execution-id", parent_id.to_string())
        .send()
        .await
        .expect("webhook request failed");
    assert_eq!(resp.status().as_u16(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(
        body["executionStatus"]["status"],
        serde_json::json!("cancelling")
    );
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
    assert_eq!(body["result"], 12);
    server.shutdown().await;
}

// ---- Webhook: ES module import calling activity ----

#[tokio::test]
async fn webhook_js_import_call_activity() {
    let server = TestServer::start(test_addr!(72)).await;
    let resp = server
        .client
        .get(format!("{}/import-call-activity", server.webhook_base_url))
        .send()
        .await
        .expect("webhook request failed");
    assert_eq!(resp.status().as_u16(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["result"], 12);
    server.shutdown().await;
}

// ---- Webhook: ES module schedule import ----

#[tokio::test]
async fn webhook_js_import_schedule_activity() {
    let server = TestServer::start(test_addr!(73)).await;
    let resp = server
        .client
        .get(format!(
            "{}/import-schedule-activity",
            server.webhook_base_url
        ))
        .send()
        .await
        .expect("webhook request failed");
    assert_eq!(resp.status().as_u16(), 200);
    let body: Value = resp.json().await.unwrap();
    assert!(
        body["execId"].is_string(),
        "expected execId to be a string, got: {body}"
    );
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

/// Mirrors the unit test `webhook_js_generate_execution_id` in `webhook_trigger.rs`.
/// JS source (in `generate_execution_id.js`):
/// ```js
/// export default function handle(request) {
///     const id1 = obelisk.generateExecutionId();
///     const id2 = obelisk.generateExecutionId();
///     return Response.json({
///         id1,
///         id2,
///         different: id1 !== id2,
///         hasPrefix: id1.startsWith("E_"),
///     });
/// }
/// ```
#[tokio::test]
async fn webhook_js_generate_execution_id() {
    let server = TestServer::start(test_addr!(43)).await;
    let resp = server
        .client
        .get(format!("{}/generate-execution-id", server.webhook_base_url))
        .send()
        .await
        .expect("webhook request failed");
    assert_eq!(resp.status().as_u16(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["different"], json!(true));
    assert_eq!(body["hasPrefix"], json!(true));
    // Verify the IDs are valid execution IDs
    let id1 = body["id1"].as_str().expect("id1 must be a string");
    let id2 = body["id2"].as_str().expect("id2 must be a string");
    assert!(id1.starts_with("E_"), "id1 must have E_ prefix, got: {id1}");
    assert!(id2.starts_with("E_"), "id2 must have E_ prefix, got: {id2}");
    assert_ne!(id1, id2, "generated execution IDs must be unique");
    server.shutdown().await;
}

// ---- Request body access ----

#[tokio::test]
async fn webhook_js_request_body_text() {
    let server = TestServer::start(test_addr!(44)).await;
    let resp = server
        .client
        .post(format!("{}/body-text", server.webhook_base_url))
        .body("hello from body")
        .send()
        .await
        .expect("webhook request failed");
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(resp.text().await.unwrap(), "hello from body");
    server.shutdown().await;
}

#[tokio::test]
async fn webhook_js_request_body_json() {
    let server = TestServer::start(test_addr!(45)).await;
    let resp = server
        .client
        .post(format!("{}/body-json", server.webhook_base_url))
        .header("content-type", "application/json")
        .body(r#"{"name":"world","value":42}"#)
        .send()
        .await
        .expect("webhook request failed");
    assert_eq!(resp.status().as_u16(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["received"]["name"], "world");
    assert_eq!(body["received"]["value"], 42);
    server.shutdown().await;
}

#[tokio::test]
async fn webhook_js_request_body_form_data() {
    let server = TestServer::start(test_addr!(46)).await;
    let resp = server
        .client
        .post(format!("{}/body-form-data", server.webhook_base_url))
        .header("content-type", "application/x-www-form-urlencoded")
        .body("name=Alice&city=Wonderland")
        .send()
        .await
        .expect("webhook request failed");
    assert_eq!(resp.status().as_u16(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["name"], "Alice");
    assert_eq!(body["city"], "Wonderland");
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
async fn hot_redeploy_activity_impl(server: &TestServer, deploy_client: &TestDeployClient) {
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
    deploy_client
        .submit_and_hot_redeploy(server, |doc| {
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

#[tokio::test]
async fn hot_redeploy_activity_grpc() {
    let server = TestServer::start(test_addr!(30)).await;
    hot_redeploy_activity_impl(&server, &TestDeployClient::Grpc).await;
    server.shutdown().await;
}

#[tokio::test]
async fn hot_redeploy_activity_webapi() {
    let server = TestServer::start(test_addr!(39)).await;
    hot_redeploy_activity_impl(&server, &TestDeployClient::WebApi).await;
    server.shutdown().await;
}

/// After a hot redeploy, both the gRPC server and the web API server must expose
/// the updated component registry.
///
/// A new inline stub `testing:integration/stubs.new-hot-stub` is added only in
/// the second deployment.  The test asserts that before the hot redeploy the stub
/// is absent from both REST `/v1/functions` and gRPC `ListComponents`, and present
/// in both after it.
async fn hot_redeploy_registry_impl(server: &TestServer, deploy_client: &TestDeployClient) {
    const NEW_STUB_FFQN: &str = "testing:integration/stubs.new-hot-stub";

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
                deployment_id: None,
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

    // Build a second deployment that adds the new inline stub and hot-redeploy.
    deploy_client
        .submit_and_hot_redeploy(server, |doc| {
            append_inline_stub(doc, "new_hot_stub", NEW_STUB_FFQN);
        })
        .await;

    // Both servers must now expose the updated registry.
    assert!(
        rest_has_new_stub().await,
        "stub must be present after hot redeploy (REST)"
    );
    assert!(
        grpc_has_new_stub(grpc_endpoint).await,
        "stub must be present after hot redeploy (gRPC)"
    );
}

#[tokio::test]
async fn hot_redeploy_registry_grpc() {
    let server = TestServer::start(test_addr!(31)).await;
    hot_redeploy_registry_impl(&server, &TestDeployClient::Grpc).await;
    server.shutdown().await;
}

#[tokio::test]
async fn hot_redeploy_registry_webapi() {
    let server = TestServer::start(test_addr!(40)).await;
    hot_redeploy_registry_impl(&server, &TestDeployClient::WebApi).await;
    server.shutdown().await;
}

/// After a hot redeploy, an existing JS webhook endpoint must pick up the new
/// env-var value from the updated deployment — proving that `WebhookServerState`
/// (including `fn_registry`, `deployment_id`, and the rebuilt router) is pushed
/// through the `WebhookRegistry` watch channel.
async fn hot_redeploy_webhook_js_env_var_impl(
    server: &TestServer,
    deploy_client: &TestDeployClient,
) {
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
    deploy_client
        .submit_and_hot_redeploy(server, |doc| {
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

#[tokio::test]
async fn hot_redeploy_webhook_js_env_var_grpc() {
    let server = TestServer::start(test_addr!(32)).await;
    hot_redeploy_webhook_js_env_var_impl(&server, &TestDeployClient::Grpc).await;
    server.shutdown().await;
}

#[tokio::test]
async fn hot_redeploy_webhook_js_env_var_webapi() {
    let server = TestServer::start(test_addr!(41)).await;
    hot_redeploy_webhook_js_env_var_impl(&server, &TestDeployClient::WebApi).await;
    server.shutdown().await;
}

/// After a hot redeploy that removes a JS webhook endpoint, the route must
/// return 404 — proving that the router inside the running HTTP server is
/// replaced, not just the env vars.
async fn hot_redeploy_webhook_js_remove_endpoint_impl(
    server: &TestServer,
    deploy_client: &TestDeployClient,
) {
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
    deploy_client
        .submit_and_hot_redeploy(server, |doc| {
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

#[tokio::test]
async fn hot_redeploy_webhook_js_remove_endpoint_grpc() {
    let server = TestServer::start(test_addr!(33)).await;
    hot_redeploy_webhook_js_remove_endpoint_impl(&server, &TestDeployClient::Grpc).await;
    server.shutdown().await;
}

#[tokio::test]
async fn hot_redeploy_webhook_js_remove_endpoint_webapi() {
    let server = TestServer::start(test_addr!(42)).await;
    hot_redeploy_webhook_js_remove_endpoint_impl(&server, &TestDeployClient::WebApi).await;
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
    assert!(
        body["wasm_backtrace"]["frames"].is_array(),
        "wasm_backtrace.frames must be an array"
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

    // Execution log must contain Persist events for Math.random() calls.
    // API response shape: { "events": [{ "event": { "history_event": { "event": { "type": "persist", ... } } } }], ... }
    let events_resp = server.get_events(&exec_id).await;
    let has_persist = events_resp["events"]
        .as_array()
        .unwrap()
        .iter()
        .any(|e| e["event"]["history_event"]["event"]["type"].as_str() == Some("persist"));
    assert!(
        has_persist,
        "expected at least one Persist event for Math.random(), got: {events_resp}"
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

    // Execution log must contain a JoinSetRequest::DelayRequest event from the
    // internal sleep call that Date.now() uses via sleep_bt(Now).
    // API response shape: { "events": [{ "event": { "history_event": { "event": { "type": "join_set_request", ... } } } }], ... }
    let events_resp = server.get_events(&exec_id).await;
    let has_delay_request =
        events_resp["events"].as_array().unwrap().iter().any(|e| {
            e["event"]["history_event"]["event"]["type"].as_str() == Some("join_set_request")
        });
    assert!(
        has_delay_request,
        "expected at least one JoinSetRequest::DelayRequest event for Date.now(), got: {events_resp}"
    );

    // Replay must produce the same result — Date.now() uses the persisted clock
    let replay_resp = server.replay(&exec_id).await;
    assert_eq!(replay_resp.status().as_u16(), 200);

    server.shutdown().await;
}

// ---- Activity exec: native process activities ----

#[tokio::test]
async fn activity_exec_add() {
    let server = TestServer::start(test_addr!(50)).await;
    let resp = server
        .submit_follow("testing:integration/exec-add.add", vec![json!(3), json!(5)])
        .await;
    assert_eq!(resp.status().as_u16(), 201);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body, json!({ "ok": 8 }));
    server.shutdown().await;
}

#[tokio::test]
async fn activity_exec_greet_inline() {
    activity_exec_greet(test_addr!(52), "inline").await;
}

#[tokio::test]
async fn activity_exec_greet_include() {
    activity_exec_greet(test_addr!(53), "include").await;
}

async fn activity_exec_greet(ip: String, suffix: &str) {
    let server = TestServer::start(ip).await;
    let resp = server
        .submit_follow(
            &format!("testing:integration/exec-greet.greet-{suffix}"),
            vec![json!("World")],
        )
        .await;
    assert_eq!(resp.status().as_u16(), 201);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body, json!({ "ok": "Hello, World!" }));
    server.shutdown().await;
}

#[tokio::test]
async fn activity_exec_record_return_type() {
    let server = TestServer::start(test_addr!(54)).await;
    let resp = server
        .submit_follow("testing:integration/exec-record.make-record", vec![])
        .await;
    assert_eq!(resp.status().as_u16(), 201);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body, json!({ "ok": { "name": "Alice", "count": 42 } }));
    server.shutdown().await;
}

#[tokio::test]
async fn activity_exec_stdin_secrets() {
    let server = TestServer::start(test_addr!(55)).await;
    let resp = server
        .submit_follow("testing:integration/exec-stdin.expose-secrets", vec![])
        .await;
    assert_eq!(resp.status().as_u16(), 201);
    let body: Value = resp.json().await.unwrap();
    // Secrets are serialized as a JSON object under the `secrets` key to stdin; the script wraps it as a JSON string.
    let ok_val = body["ok"].as_str().expect("expected ok string");
    let parsed: Value = serde_json::from_str(ok_val).expect("inner value must be valid JSON");
    assert_eq!(
        parsed,
        json!({ "secrets": { "MY_SECRET": "s3cret_value" } })
    );
    server.shutdown().await;
}

#[tokio::test]
async fn activity_exec_void_ok() {
    let server = TestServer::start(test_addr!(56)).await;
    let resp = server
        .submit_follow("testing:integration/exec-void.void-ok", vec![])
        .await;
    assert_eq!(resp.status().as_u16(), 201);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body, json!({ "ok": null }));
    server.shutdown().await;
}

#[tokio::test]
async fn activity_exec_void_err() {
    let server = TestServer::start(test_addr!(57)).await;
    let resp = server
        .submit_follow("testing:integration/exec-void.void-err", vec![])
        .await;
    assert_eq!(resp.status().as_u16(), 201);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body, json!({ "err": null }));
    server.shutdown().await;
}

#[tokio::test]
async fn activity_exec_args_passthrough() {
    let server = TestServer::start(test_addr!(58)).await;
    let resp = server
        .submit_follow(
            "testing:integration/exec-args.echo-args",
            vec![json!(10), json!(20)],
        )
        .await;
    assert_eq!(resp.status().as_u16(), 201);
    let body: Value = resp.json().await.unwrap();
    // The bash script receives JSON-serialized params as positional args ($1=10, $2=20)
    // and echoes them back as a JSON record: {"a": 10, "b": 20}
    assert_eq!(body, json!({ "ok": { "a": 10, "b": 20 } }));
    server.shutdown().await;
}

#[tokio::test]
async fn activity_exec_args_via_stdin() {
    let server = TestServer::start(test_addr!(82)).await;
    let resp = server
        .submit_follow(
            "testing:integration/exec-stdin-args.echo-args",
            vec![json!(10), json!(20)],
        )
        .await;
    assert_eq!(resp.status().as_u16(), 201);
    let body: Value = resp.json().await.unwrap();
    // With params_via_stdin, params arrive in the stdin JSON `params` array
    // (argv carries none); the script echoes them back as {"a": 10, "b": 20}.
    assert_eq!(body, json!({ "ok": { "a": 10, "b": 20 } }));
    server.shutdown().await;
}

#[tokio::test]
async fn activity_exec_env_vars() {
    let server = TestServer::start(test_addr!(59)).await;
    let resp = server
        .submit_follow("testing:integration/exec-env.read-env", vec![])
        .await;
    assert_eq!(resp.status().as_u16(), 201);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body, json!({ "ok": "hello_from_exec_env" }));
    server.shutdown().await;
}

#[tokio::test]
async fn activity_exec_error_exit() {
    let server = TestServer::start(test_addr!(60)).await;
    let resp = server
        .submit_follow("testing:integration/exec-error.fail", vec![])
        .await;
    assert_eq!(resp.status().as_u16(), 201);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body, json!({ "err": "something went wrong" }));
    server.shutdown().await;
}

#[tokio::test]
async fn activity_exec_stream_logs() {
    let server = TestServer::start(test_addr!(61)).await;
    let exec_id = server.generate_execution_id().await;

    info!("About to submit the execution");
    let resp = server
        .submit_follow_with_id(
            &exec_id,
            "testing:integration/exec-stream.stream-test",
            vec![],
        )
        .await;
    assert_eq!(resp.status().as_u16(), 201);

    let body: Value = resp.json().await.unwrap();
    assert_eq!(body, json!({ "ok": null }));

    let stderr_entries = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let logs = server.get_logs(&exec_id).await;
            debug!("Fetched logs: {logs:?}");
            let stderr_entries: Vec<Value> = logs
                .as_array()
                .expect("logs must be an array")
                .iter()
                .filter(|entry| entry["type"] == "stream" && entry["stream_type"] == "stderr")
                .cloned()
                .collect();
            if stderr_entries.len() >= 2 {
                break stderr_entries;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("timed out waiting for stderr stream entries");

    // Streaming must produce 2 separate stderr entries (one per echo).
    assert_eq!(
        2,
        stderr_entries.len(),
        "expected 2 stderr stream entries, got {}: {stderr_entries:?}",
        stderr_entries.len(),
    );
    server.shutdown().await;
}
