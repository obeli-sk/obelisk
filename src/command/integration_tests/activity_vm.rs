use crate::command::server;
use crate::{command::server::ActivityVmRuntimeMode, parse_activity_vm_runtime_from_env};
use std::sync::Arc;

use super::*;

async fn start_vm_server(
    ip: String,
    server_toml: &str,
    deployment_toml: &str,
    sleep: Option<Arc<dyn concepts::time::Sleep>>,
) -> TestServer {
    let (persisted_logs, _) = tokio::sync::broadcast::channel(1000);
    TestServer::start_inline_deployment_with_hooks(
        ip,
        server_toml,
        deployment_toml,
        &[],
        JsRuntimeMode::BoaWasm,
        server::RuntimeTestHooks {
            activity_vm_sleep: sleep,
            persisted_logs: Some(persisted_logs),
        },
    )
    .await
}

async fn wait_for_vm_output(
    logs: &mut tokio::sync::broadcast::Receiver<concepts::storage::LogInfoAppendRow>,
    execution_id: &str,
    expected_stdout: Option<&str>,
    expected_stderr: &str,
) -> (String, String) {
    use concepts::storage::{LogEntry, LogStreamType};
    let mut stdout = String::new();
    let mut stderr = String::new();
    loop {
        let row = logs
            .recv()
            .await
            .expect("persisted log observer must remain connected");
        if row.execution_id.to_string() == execution_id
            && let LogEntry::Stream {
                payload,
                stream_type,
                ..
            } = row.log_entry
        {
            match stream_type {
                LogStreamType::StdOut => stdout.push_str(&String::from_utf8_lossy(&payload)),
                LogStreamType::StdErr => stderr.push_str(&String::from_utf8_lossy(&payload)),
            }
            if expected_stdout.is_none_or(|expected| stdout == expected)
                && stderr.contains(expected_stderr)
            {
                break (stdout, stderr);
            }
        }
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
memory.mib = 512
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
async fn clock_and_entropy_fresh_after_resume() {
    if parse_activity_vm_runtime_from_env(&StartupEnvVars::capture()).unwrap()
        == ActivityVmRuntimeMode::Disabled
    {
        return;
    }
    let deployment_toml = r#"[[activity_vm]]
memory.mib = 512
exec.lock_expiry.seconds = 120
ffqn = "testing:vm/clock.run"
content = '''#!/usr/bin/env bash
printf '"%(%s)T %s"\n' -1 "$SRANDOM"
'''
params = []
return_type = "result<string, string>"
store_paths = ["/nix/store/2ndah67h0z5m31v2wkdmg2md4380ggr5-bash-interactive-5.3p15"]
"#;
    let server =
        TestServer::start_inline_deployment(test_addr!(183), "", deployment_toml, &[]).await;
    let mut randoms = Vec::new();
    for _ in 0..2 {
        let response = server.submit_follow("testing:vm/clock.run", vec![]).await;
        assert_eq!(response.status().as_u16(), 201);
        let output = response.json::<Value>().await.unwrap()["ok"]
            .as_str()
            .expect("ok string")
            .to_owned();
        let (guest_seconds, random) = output.split_once(' ').unwrap();
        let host_seconds = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let skew = host_seconds.abs_diff(guest_seconds.parse().unwrap());
        assert!(skew < 60, "guest clock is off by {skew} s");
        randoms.push(random.to_owned());
    }
    assert_ne!(randoms[0], randoms[1], "guest CRNG replayed the snapshot");
    server.shutdown().await;
}

#[tokio::test]
async fn emubench_all() {
    let deployment_toml = r#"[[activity_vm]]
memory.mib = 512
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
memory.mib = 512
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
memory.mib = 512
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
async fn stdout_and_stderr_forwarded_to_logs() {
    if parse_activity_vm_runtime_from_env(&StartupEnvVars::capture()).unwrap()
        == ActivityVmRuntimeMode::Disabled
    {
        return;
    }
    let deployment_toml = r#"[[activity_vm]]
memory.mib = 512
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
    let mut server = start_vm_server(test_addr!(140), "", deployment_toml, None).await;
    let mut logs = server.persisted_logs.take().unwrap();
    let exec_id = server.generate_execution_id().await;
    let response = server
        .submit_follow_with_id(&exec_id, "testing:vm/stderr.run", vec![])
        .await;
    assert_eq!(response.status().as_u16(), 201);
    // stderr must not corrupt the JSON result parsed from stdout.
    assert_eq!(
        response.json::<Value>().await.unwrap(),
        json!({ "ok": "stdout-result" })
    );

    let (stdout, stderr) = tokio::time::timeout(
        Duration::from_secs(10),
        wait_for_vm_output(
            &mut logs,
            &exec_id,
            Some("\"stdout-result\"\n"),
            "guest diagnostic\n",
        ),
    )
    .await
    .expect("guest output must be persisted");
    assert_eq!(stdout, "\"stdout-result\"\n");
    assert!(stderr.contains("guest diagnostic\n"), "{stderr:?}");
    assert_eq!(stream_log(&server, &exec_id, "stdout").await, stdout);
    assert!(
        stream_log(&server, &exec_id, "stderr")
            .await
            .contains("guest diagnostic\n")
    );
    server.shutdown().await;
}

/// Concatenates the payloads of one stream type.
async fn stream_log(server: &TestServer, execution_id: &str, stream_type: &str) -> String {
    use base64::prelude::*;
    let logs = server.get_logs(execution_id, 1000).await;
    logs.as_array()
        .expect("logs must be an array")
        .iter()
        .filter(|entry| entry["type"] == "stream" && entry["stream_type"] == stream_type)
        .map(|entry| {
            let payload = BASE64_STANDARD
                .decode(entry["payload"].as_str().expect("payload must be a string"))
                .expect("payload must be base64");
            String::from_utf8_lossy(&payload).into_owned()
        })
        .collect()
}

#[tokio::test]
async fn output_forwarded_before_completion() {
    if parse_activity_vm_runtime_from_env(&StartupEnvVars::capture()).unwrap()
        == ActivityVmRuntimeMode::Disabled
    {
        return;
    }
    let deployment_toml = r#"[[activity_vm]]
memory.mib = 512
exec.lock_expiry.seconds = 120
max_retries = 0
ffqn = "testing:vm/hang.run"
content = '''#!/usr/bin/env bash
printf '%s\n' 'before hang' >&2
exec sleep 600
'''
params = []
return_type = "result<string, string>"
store_paths = ["/nix/store/2ndah67h0z5m31v2wkdmg2md4380ggr5-bash-interactive-5.3p15"]
"#;
    let mut server = start_vm_server(test_addr!(186), "", deployment_toml, None).await;
    let mut logs = server.persisted_logs.take().unwrap();
    let execution_id = server.generate_execution_id().await;
    let follow = server.submit_follow_with_id(&execution_id, "testing:vm/hang.run", vec![]);
    let observe_and_cancel = async {
        wait_for_vm_output(&mut logs, &execution_id, None, "before hang\n").await;
        assert!(
            stream_log(&server, &execution_id, "stderr")
                .await
                .contains("before hang\n")
        );
        server.cancel_execution_with_retries(&execution_id).await;
    };
    let (response, ()) = tokio::time::timeout(Duration::from_secs(60), async {
        tokio::join!(follow, observe_and_cancel)
    })
    .await
    .expect("stderr must reach the logs while the VM is still running");
    assert_eq!(
        response.json::<Value>().await.unwrap(),
        json!({ "execution_failed": { "kind": "cancelled" } })
    );
    server.shutdown().await;
}

#[tokio::test]
async fn nonzero_exit_maps_error_result() {
    let deployment_toml = r#"[[activity_vm]]
memory.mib = 512
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
memory.mib = 512
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
    Box::pin(activity_vm_http_case(test_addr!(138), false)).await;
}

#[tokio::test]
async fn http_obelisk_host() {
    Box::pin(activity_vm_http_case(test_addr!(139), true)).await;
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
memory.mib = 512
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

fn vm_processes() -> Vec<u32> {
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
            let is_vmm = |name: &[u8]| command.windows(name.len()).any(|part| part == name);
            (parent == parent_pid && (is_vmm(b"qemu-system-x86_64") || is_vmm(b"firecracker")))
                .then_some(pid)
        })
        .collect()
}

async fn vm_interruption_case(ip: String, cancel: bool) {
    use tokio::io::{AsyncBufReadExt as _, BufReader};

    if !matches!(
        parse_activity_vm_runtime_from_env(&StartupEnvVars::capture()).unwrap(),
        ActivityVmRuntimeMode::QemuTcg
            | ActivityVmRuntimeMode::QemuKvm
            | ActivityVmRuntimeMode::Firecracker
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
    let deployment_toml = format!(
        r#"[[activity_vm]]
memory.mib = 512
ffqn = "testing:vm/hang.run"
exec.lock_expiry.seconds = 120
max_retries = 0
content = '''#!/usr/bin/env bash
set -eu
curl -fsS --connect-to {authority}:127.0.0.1:80 http://{authority}/ready
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
    let deadline = Arc::new(test_utils::manual_sleep::ManualSleep::default());
    let server = start_vm_server(ip, &server_toml, &deployment_toml, Some(deadline.clone())).await;
    let execution_id = server.generate_execution_id().await;
    let follow = server.submit_follow_with_id(&execution_id, "testing:vm/hang.run", vec![]);
    let observe_ready = async {
        let (stream, _) = listener.accept().await.unwrap();
        let mut stream = BufReader::new(stream);
        let mut request = Vec::new();
        stream.read_until(b'\n', &mut request).await.unwrap();
        assert_eq!(request, b"GET /ready HTTP/1.1\r\n");
        let children = vm_processes();
        assert_eq!(children.len(), 1, "expected one running VM: {children:?}");
        (children[0], stream)
    };
    let (response, pid) = tokio::time::timeout(Duration::from_secs(60), async {
        tokio::pin!(follow);
        let (pid, _request) = tokio::select! {
            pid = observe_ready => pid,
            response = &mut follow => panic!(
                "VM activity finished before readiness: {}",
                response.data,
            ),
        };
        if cancel {
            server.cancel_execution_with_retries(&execution_id).await;
        } else {
            deadline.expire();
        }
        (follow.await, pid)
    })
    .await
    .expect("VM activity did not finish after interruption");
    assert_eq!(response.status().as_u16(), 201);
    let expected_kind = if cancel { "cancelled" } else { "timed_out" };
    assert_eq!(
        response.json::<Value>().await.unwrap(),
        json!({ "execution_failed": { "kind": expected_kind } })
    );
    assert!(
        !std::path::Path::new(&format!("/proc/{pid}")).exists(),
        "VM process {pid} survived {expected_kind}",
    );
    server.shutdown().await;
}

#[tokio::test]
async fn cancellation_kills_vm() {
    vm_interruption_case(test_addr!(181), true).await;
}

#[tokio::test]
async fn lock_expiry_kills_vm() {
    vm_interruption_case(test_addr!(182), false).await;
}

#[tokio::test]
async fn guest_memory() {
    if !matches!(
        parse_activity_vm_runtime_from_env(&StartupEnvVars::capture()).unwrap(),
        ActivityVmRuntimeMode::QemuTcg
            | ActivityVmRuntimeMode::QemuKvm
            | ActivityVmRuntimeMode::Firecracker
    ) {
        return;
    }
    // The fill exceeds what the 256 MiB snapshot could hold in /tmp, so the rootfs must grow with the RAM.
    let deployment_toml = r#"[[activity_vm]]
exec.lock_expiry.seconds = 120
ffqn = "testing:vm/memory.run"
content = '''#!/usr/bin/env bash
set -eu
while read -r key value _; do
  [[ $key == MemTotal: ]] && total=$value
done < /proc/meminfo
head -c 300000000 /dev/zero > /tmp/fill
printf '%s\n' "$total"
'''
params = []
return_type = "result<u64, string>"
store_paths = ["/nix/store/2ndah67h0z5m31v2wkdmg2md4380ggr5-bash-interactive-5.3p15"]
memory.gib = 2
"#;
    let server =
        TestServer::start_inline_deployment(test_addr!(184), "", deployment_toml, &[]).await;
    let response = server.submit_follow("testing:vm/memory.run", vec![]).await;
    assert_eq!(response.status().as_u16(), 201);
    let body = response.json::<Value>().await.unwrap();
    let total_kib = body["ok"].as_u64().unwrap_or_else(|| panic!("{body}"));
    assert!(total_kib > 1900 * 1024, "guest MemTotal is {total_kib} KiB");
    server.shutdown().await;
}

#[tokio::test]
async fn guest_cpus() {
    if !matches!(
        parse_activity_vm_runtime_from_env(&StartupEnvVars::capture()).unwrap(),
        ActivityVmRuntimeMode::QemuTcg
            | ActivityVmRuntimeMode::QemuKvm
            | ActivityVmRuntimeMode::Firecracker
    ) {
        return;
    }
    let deployment_toml = r#"[[activity_vm]]
exec.lock_expiry.seconds = 120
ffqn = "testing:vm/cpus.run"
content = '''#!/usr/bin/env bash
set -eu
count=0
while read -r key _; do
  [[ $key == processor ]] && count=$((count + 1))
done < /proc/cpuinfo
printf '%s\n' "$count"
'''
params = []
return_type = "result<u64, string>"
store_paths = ["/nix/store/2ndah67h0z5m31v2wkdmg2md4380ggr5-bash-interactive-5.3p15"]
memory.mib = 256
cpus = 4
"#;
    let server =
        TestServer::start_inline_deployment(test_addr!(185), "", deployment_toml, &[]).await;
    let response = server.submit_follow("testing:vm/cpus.run", vec![]).await;
    assert_eq!(response.status().as_u16(), 201);
    let body = response.json::<Value>().await.unwrap();
    assert_eq!(body["ok"].as_u64(), Some(4), "{body}");
    server.shutdown().await;
}
