use super::{API_PORT, WEBHOOK_PORT, get_workspace_dir};
use std::path::PathBuf;

pub(super) fn write_server_config(
    ip: &str,
    server_toml_api_lines: &str,
    server_toml_tail: &str,
) -> (tempfile::TempDir, PathBuf, PathBuf) {
    let workspace = get_workspace_dir();
    let tmp_dir = tempfile::tempdir().unwrap();
    let server_contents = format!(
        r#"api.listening_addr = "{ip}:{API_PORT}"
{server_toml_api_lines}
webui.enabled = false
external.listening_addr = "{ip}:{WEBHOOK_PORT}"

[wasm]
cache_directory = "{wasm_cache}"

[wasm.codegen_cache]
directory = "{codegen_cache}"

[database.sqlite]
directory = "{tmp_dir}"

[public_env]
allowed = ["PATH", "OBELISK_PHASE5_DEFINITELY_MISSING_VAR"]

[secrets]
MY_SECRET = {{ env = "MY_SECRET" }}
VM_SECRET = {{ env = "VM_SECRET" }}

[[outbound_http.allowed_host]]
pattern = "*"
methods = "*"

{server_toml_tail}
"#,
        codegen_cache = workspace.join("test-codegen-cache").display(),
        wasm_cache = workspace.join("test-wasm-cache").display(),
        tmp_dir = tmp_dir.path().display(),
    );
    let server_path = tmp_dir.path().join("server.toml");
    let deployment_path = tmp_dir.path().join("deployment.toml");
    std::fs::write(&server_path, server_contents).unwrap();
    (tmp_dir, server_path, deployment_path)
}
