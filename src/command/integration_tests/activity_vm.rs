use super::*;

impl TestServer {
    async fn start_activity_vm(ip: String, server_toml_lines: &str, deployment_toml: &str) -> Self {
        let (tmp_dir, server_path, deployment_path) =
            util::write_server_config(&ip, "", server_toml_lines);
        std::fs::write(&deployment_path, deployment_toml).unwrap();
        let deployment = LocalDeployment::from_path(&deployment_path).await.unwrap();
        Self::launch(ip, tmp_dir, server_path, deployment, true, None).await
    }
}

async fn activity_vm_case(
    ip: String,
    server_toml: &str,
    deployment_toml: &str,
    ffqn: &str,
    params: Vec<Value>,
    expected: Value,
) {
    let server = TestServer::start_activity_vm(ip, server_toml, deployment_toml).await;
    let response = server.submit_follow(ffqn, params).await;
    assert_eq!(response.status().as_u16(), 201, "submitting {ffqn}");
    assert_eq!(response.json::<Value>().await.unwrap(), expected, "{ffqn}");
    server.shutdown().await;
}

#[tokio::test]
async fn echo() {
    let server_toml = "";
    let deployment_toml = r#"[[activity_vm]]
exec.lock_expiry.seconds = 300
ffqn = "testing:vm/echo.run"
content = '''#!/bin/sh
/bin/echo '"Hello, world!"'
'''
params = []
return_type = "result<string, string>"
store_paths = []
"#
    .to_string();
    activity_vm_case(
        test_addr!(135),
        server_toml,
        &deployment_toml,
        "testing:vm/echo.run",
        vec![],
        json!({ "ok": "Hello, world!" }),
    )
    .await;
}

#[tokio::test]
async fn entrypoint() {
    let server_toml = "";
    let deployment_toml = r#"[[activity_vm]]
exec.lock_expiry.seconds = 120
ffqn = "testing:vm/entrypoint.run"
entrypoint = ["/bin/sh", "-c", "printf '%s' '\"entrypoint\"'"]
params = []
return_type = "result<string, string>"
store_paths = []
"#
    .to_string();
    activity_vm_case(
        test_addr!(136),
        server_toml,
        &deployment_toml,
        "testing:vm/entrypoint.run",
        vec![],
        json!({ "ok": "entrypoint" }),
    )
    .await;
}

#[tokio::test]
async fn stdin() {
    let server_toml = "";
    let deployment_toml = r#"[[activity_vm]]
exec.lock_expiry.seconds = 120
ffqn = "testing:vm/stdin.run"
content = '''#!/bin/sh
set -eu
input=$(cat)
case "$input:$VM_SECRET:$VM_MODE" in
  '{"params":["payload"]}:swordfish:testing') printf '%s\n' '"stdin-and-secret"' ;;
  *) printf '%s\n' '"unexpected stdin"'; exit 1 ;;
esac
'''
params = [{ name = "input", type = "string" }]
return_type = "result<string, string>"
params_via_stdin = true
exposed_secrets = ["VM_SECRET"]
env_vars = [{ key = "VM_MODE", value = "testing" }]
store_paths = []
"#
    .to_string();
    activity_vm_case(
        test_addr!(137),
        server_toml,
        &deployment_toml,
        "testing:vm/stdin.run",
        vec![json!("payload")],
        json!({ "ok": "stdin-and-secret" }),
    )
    .await;
}

#[tokio::test]
async fn stderr_does_not_corrupt_json_result() {
    let deployment_toml = r#"[[activity_vm]]
exec.lock_expiry.seconds = 120
ffqn = "testing:vm/stderr.run"
content = '''#!/bin/sh
printf '%s\n' 'guest diagnostic' >&2
printf '%s\n' '"stdout-result"'
'''
params = []
return_type = "result<string, string>"
store_paths = []
"#;
    activity_vm_case(
        test_addr!(140),
        "",
        deployment_toml,
        "testing:vm/stderr.run",
        vec![],
        json!({ "ok": "stdout-result" }),
    )
    .await;
}

#[tokio::test]
#[ignore = "QEMU nonzero exit propagation requires follow-up"]
async fn nonzero_exit_maps_error_result() {
    let deployment_toml = r#"[[activity_vm]]
ffqn = "testing:vm/failure.run"
max_retries = 0
content = '''#!/bin/sh
printf '%s\n' '"expected-failure"'
exit 7
'''
params = []
return_type = "result<string, string>"
store_paths = []
"#;
    activity_vm_case(
        test_addr!(141),
        "",
        deployment_toml,
        "testing:vm/failure.run",
        vec![],
        json!({ "err": "expected-failure" }),
    )
    .await;
}

async fn activity_vm_http_case(ip: String, use_host_alias: bool) {
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{header, method, path},
    };

    let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let mock_addr = listener.local_addr().unwrap();
    let mock_port = mock_addr.port();
    let mock = MockServer::builder().listener(listener).start().await;
    Mock::given(method("GET"))
        .and(path("/anything"))
        .and(header("X-VM-Secret", "swordfish"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let policy_authority = format!("localhost:{mock_port}");
    let request_authority = if use_host_alias {
        format!("obelisk-host:{mock_port}")
    } else {
        policy_authority.clone()
    };
    let connect_to = if use_host_alias {
        "  --noproxy '*' \\\n".to_string()
    } else {
        format!("  --noproxy '*' \\\n  --connect-to {request_authority}:127.0.0.1:80 \\\n")
    };

    let server_toml = format!(
        r#"[[outbound_http.allowed_host]]
pattern = "http://{policy_authority}"
methods = ["GET"]
secrets = ["VM_SECRET"]
replace_in = ["headers"]
"#
    );
    let deployment_toml = format!(
        r#"[[activity_vm]]
exec.lock_expiry.seconds = 120
ffqn = "testing:vm/http.run"
content = '''#!/usr/bin/env bash
set -eu
case "$VM_SECRET" in
  OBELISK_SECRET_*) ;;
  *) printf '%s\n' '"VM received the secret value"'; exit 1 ;;
esac
curl -fsS \
{connect_to}  --connect-timeout 5 \
  --max-time 10 \
  -H "X-VM-Secret: ${{VM_SECRET}}" \
  http://{request_authority}/anything
printf '%s\n' '"secret-rewritten"'
'''
params = []
return_type = "result<string, string>"
store_paths = [
  "/nix/store/2ndah67h0z5m31v2wkdmg2md4380ggr5-bash-interactive-5.3p15",
  "/nix/store/cp8qnyl8i0s62g3a1465i258mf5bcr6k-curl-8.22.0-bin",
]
[[activity_vm.allowed_host]]
pattern = "http://{policy_authority}"
methods = ["GET"]
secrets = ["VM_SECRET"]
replace_in = ["headers"]
"#,
    );
    let server = TestServer::start_activity_vm(ip, &server_toml, &deployment_toml).await;
    let response = server.submit_follow("testing:vm/http.run", vec![]).await;
    assert_eq!(response.status().as_u16(), 201);
    assert_eq!(
        response.json::<Value>().await.unwrap(),
        json!({ "ok": "secret-rewritten" })
    );
    server.shutdown().await;
}

#[tokio::test]
#[ignore = "QEMU HTTP bridge requires writable 9p follow-up"]
async fn http_loopback_connect_to() {
    activity_vm_http_case(test_addr!(138), false).await;
}

#[tokio::test]
#[ignore = "QEMU HTTP bridge requires writable 9p follow-up"]
async fn http_obelisk_host() {
    activity_vm_http_case(test_addr!(139), true).await;
}
