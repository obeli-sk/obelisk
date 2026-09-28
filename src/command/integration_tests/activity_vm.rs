use crate::{command::server::ActivityVmRuntimeMode, parse_activity_vm_runtime_from_env};

use super::*;

async fn activity_vm_case(
    ip: String,
    server_toml: &str,
    deployment_toml: &str,
    ffqn: &str,
    params: Vec<Value>,
    expected: Value,
) {
    if parse_activity_vm_runtime_from_env(&StartupEnvVars::capture()).unwrap()
        == ActivityVmRuntimeMode::Disabled
    {
        return;
    }
    let server = TestServer::start_inline_deployment(ip, server_toml, deployment_toml, &[]).await;
    let response = server.submit_follow(ffqn, params).await;
    assert_eq!(response.status().as_u16(), 201, "submitting {ffqn}");
    assert_eq!(response.json::<Value>().await.unwrap(), expected, "{ffqn}");
    server.shutdown().await;
}

#[tokio::test]
async fn echo() {
    let server_toml = "";
    let deployment_toml = r#"[[activity_vm]]
exec.lock_expiry.seconds = 120
ffqn = "testing:vm/echo.run"
content = '''#!/usr/bin/env bash
printf '"'
hello | tr -d '\n'
printf '"\n'
'''
params = []
return_type = "result<string, string>"
store_paths = [
  "/nix/store/2ndah67h0z5m31v2wkdmg2md4380ggr5-bash-interactive-5.3p15",
  "/nix/store/xl1h9i29pgq2q5cszjhm5wpfxfbbqwyi-hello-2.12.3",
]
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
async fn emubench_all() {
    let deployment_toml = r#"[[activity_vm]]
exec.lock_expiry.seconds = 120
ffqn = "testing:vm/emubench.run"
entrypoint = ["emubench", "all"]
params = []
return_type = "result"
store_paths = ["/nix/store/vk1ih0la8pfs92v8cy5vkv48kgmi14db-trynix-emubench-static-x86_64-unknown-linux-musl-1"]
[[activity_vm.nix_cache]]
url = "https://trynix.cachix.org"
public_key = "trynix.cachix.org-1:xmOWOHz2g/BlpCVQrTEZjSKWPk3S3Dukn1xiSWLidkY="
"#;
    activity_vm_case(
        test_addr!(178),
        "",
        deployment_toml,
        "testing:vm/emubench.run",
        vec![],
        json!({ "ok": null }),
    )
    .await;
}

#[tokio::test]
async fn entrypoint() {
    let server_toml = "";
    let deployment_toml = r#"[[activity_vm]]
exec.lock_expiry.seconds = 120
ffqn = "testing:vm/entrypoint.run"
entrypoint = ["bash", "-c", "printf '%s\\n' '\"entrypoint\"'"]
params = []
return_type = "result<string, string>"
store_paths = ["/nix/store/2ndah67h0z5m31v2wkdmg2md4380ggr5-bash-interactive-5.3p15"]
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
content = '''#!/usr/bin/env bash
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
store_paths = ["/nix/store/2ndah67h0z5m31v2wkdmg2md4380ggr5-bash-interactive-5.3p15"]
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
content = '''#!/usr/bin/env bash
printf '%s\n' 'guest diagnostic' >&2
printf '%s\n' '"stdout-result"'
'''
params = []
return_type = "result<string, string>"
store_paths = ["/nix/store/2ndah67h0z5m31v2wkdmg2md4380ggr5-bash-interactive-5.3p15"]
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
async fn nonzero_exit_maps_error_result() {
    let deployment_toml = r#"[[activity_vm]]
ffqn = "testing:vm/failure.run"
max_retries = 0
content = '''#!/usr/bin/env bash
printf '%s\n' '"expected-failure"'
exit 7
'''
params = []
return_type = "result<string, string>"
store_paths = ["/nix/store/2ndah67h0z5m31v2wkdmg2md4380ggr5-bash-interactive-5.3p15"]
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
    if parse_activity_vm_runtime_from_env(&StartupEnvVars::capture()).unwrap()
        == ActivityVmRuntimeMode::Disabled
    {
        return;
    }
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
        String::new()
    } else {
        format!("--connect-to {request_authority}:127.0.0.1:80 \\\n")
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
    let server = TestServer::start_inline_deployment(ip, &server_toml, &deployment_toml, &[]).await;
    let response = server.submit_follow("testing:vm/http.run", vec![]).await;
    assert_eq!(response.status().as_u16(), 201);
    assert_eq!(
        response.json::<Value>().await.unwrap(),
        json!({ "ok": "secret-rewritten" })
    );
    server.shutdown().await;
}

#[tokio::test]
async fn http_loopback_connect_to() {
    activity_vm_http_case(test_addr!(138), false).await;
}

#[tokio::test]
async fn http_obelisk_host() {
    activity_vm_http_case(test_addr!(139), true).await;
}

/// End-to-end (VM guest curl -> proxy -> bridge -> server): the outbound Host is derived
/// from the request authority and ordinary headers are forwarded, while a forbidden
/// header the guest sends (curl always sends Host; here also `Connection`) is stripped
/// rather than breaking the request. This is the activity-VM counterpart to the parity
/// the `http_bridge` unit tests and the JS `fetch_sets_host_header` tests assert.
async fn activity_vm_http_headers_case(ip: String) {
    if parse_activity_vm_runtime_from_env(&StartupEnvVars::capture()).unwrap()
        == ActivityVmRuntimeMode::Disabled
    {
        return;
    }
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{header, method, path},
    };

    let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let mock_port = listener.local_addr().unwrap().port();
    let mock = MockServer::builder().listener(listener).start().await;
    let authority = format!("localhost:{mock_port}");
    Mock::given(method("GET"))
        .and(path("/anything"))
        .and(header("host", authority.as_str()))
        .and(header("x-custom", "kept"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let server_toml = format!(
        r#"[[outbound_http.allowed_host]]
pattern = "http://{authority}"
methods = ["GET"]
"#
    );
    let deployment_toml = format!(
        r#"[[activity_vm]]
exec.lock_expiry.seconds = 120
ffqn = "testing:vm/http.run"
content = '''#!/usr/bin/env bash
set -eu
curl -fsS \
  --connect-to {authority}:127.0.0.1:80 \
  --connect-timeout 5 \
  --max-time 10 \
  -H "Connection: keep-alive" \
  -H "X-Custom: kept" \
  http://{authority}/anything
printf '%s\n' '"ok"'
'''
params = []
return_type = "result<string, string>"
store_paths = [
  "/nix/store/2ndah67h0z5m31v2wkdmg2md4380ggr5-bash-interactive-5.3p15",
  "/nix/store/cp8qnyl8i0s62g3a1465i258mf5bcr6k-curl-8.22.0-bin",
]
[[activity_vm.allowed_host]]
pattern = "http://{authority}"
methods = ["GET"]
"#
    );
    let server = TestServer::start_inline_deployment(ip, &server_toml, &deployment_toml, &[]).await;
    let response = server.submit_follow("testing:vm/http.run", vec![]).await;
    assert_eq!(response.status().as_u16(), 201);
    assert_eq!(
        response.json::<Value>().await.unwrap(),
        json!({ "ok": "ok" })
    );
    server.shutdown().await;
}

#[tokio::test]
async fn http_headers_host_and_forbidden() {
    activity_vm_http_headers_case(test_addr!(174)).await;
}

fn native_qemu_children() -> Vec<u32> {
    let parent_pid = std::process::id();
    std::fs::read_dir("/proc")
        .unwrap()
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let pid = entry.file_name().to_str()?.parse::<u32>().ok()?;
            let status = std::fs::read_to_string(entry.path().join("status")).ok()?;
            let parent = status
                .lines()
                .find_map(|line| line.strip_prefix("PPid:"))?
                .trim()
                .parse::<u32>()
                .ok()?;
            let command = std::fs::read(entry.path().join("cmdline")).ok()?;
            (parent == parent_pid
                && command
                    .windows(18)
                    .any(|part| part == b"qemu-system-x86_64"))
            .then_some(pid)
        })
        .collect()
}

async fn native_qemu_interruption_case(ip: String, cancel: bool) {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    if !matches!(
        parse_activity_vm_runtime_from_env(&StartupEnvVars::capture()).unwrap(),
        ActivityVmRuntimeMode::QemuTcg | ActivityVmRuntimeMode::QemuKvm
    ) {
        return;
    }
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let port = listener.local_addr().unwrap().port();
    let authority = format!("localhost:{port}");
    let server_toml = format!(
        "[[outbound_http.allowed_host]]\npattern = \"http://{authority}\"\nmethods = [\"GET\"]\n"
    );
    let lock_expiry = if cancel { 120 } else { 6 };
    let deployment_toml = format!(
        r#"[[activity_vm]]
ffqn = "testing:vm/hang.run"
exec.lock_expiry.seconds = {lock_expiry}
max_retries = 0
content = '''#!/usr/bin/env bash
set -eu
curl -fsS --connect-to {authority}:127.0.0.1:80 http://{authority}/ready
exec sleep 600
'''
params = []
return_type = "result<string, string>"
store_paths = [
  "/nix/store/2ndah67h0z5m31v2wkdmg2md4380ggr5-bash-interactive-5.3p15",
  "/nix/store/cp8qnyl8i0s62g3a1465i258mf5bcr6k-curl-8.22.0-bin",
]
[[activity_vm.allowed_host]]
pattern = "http://{authority}"
methods = ["GET"]
"#
    );
    let server = TestServer::start_inline_deployment(ip, &server_toml, &deployment_toml, &[]).await;
    let execution_id = server.generate_execution_id().await;
    let follow = server.submit_follow_with_id(&execution_id, "testing:vm/hang.run", vec![]);
    let observe_and_interrupt = async {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = [0_u8; 2048];
        let count = stream.read(&mut request).await.unwrap();
        assert!(request[..count].starts_with(b"GET /ready HTTP/1.1"));
        stream
            .write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let children = native_qemu_children();
        assert_eq!(
            children.len(),
            1,
            "expected one running native QEMU: {children:?}"
        );
        if cancel {
            server.cancel_execution_with_retries(&execution_id).await;
        }
        children[0]
    };
    let (response, pid) = tokio::time::timeout(Duration::from_secs(30), async {
        tokio::join!(follow, observe_and_interrupt)
    })
    .await
    .expect("native QEMU activity did not finish after interruption");
    assert_eq!(response.status().as_u16(), 201);
    let expected_kind = if cancel { "cancelled" } else { "timed_out" };
    assert_eq!(
        response.json::<Value>().await.unwrap(),
        json!({ "execution_failed": { "kind": expected_kind } })
    );
    tokio::time::timeout(Duration::from_secs(5), async {
        while std::path::Path::new(&format!("/proc/{pid}")).exists() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("native QEMU process {pid} survived {expected_kind}"));
    server.shutdown().await;
}

#[tokio::test]
async fn native_qemu_cancellation_kills_vm() {
    native_qemu_interruption_case(test_addr!(181), true).await;
}

#[tokio::test]
async fn native_qemu_lock_expiry_kills_vm() {
    native_qemu_interruption_case(test_addr!(182), false).await;
}
