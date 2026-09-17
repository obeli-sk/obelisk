use super::*;

async fn activity_exec_case(
    ip: String,
    deployment_toml: &str,
    files: &[(&str, &str)],
    ffqn: &str,
    params: Vec<Value>,
    expected: Value,
) {
    let server = TestServer::start_inline_deployment(ip, "", deployment_toml, files).await;
    let response = server.submit_follow(ffqn, params).await;
    assert_eq!(response.status().as_u16(), 201, "submitting {ffqn}");
    assert_eq!(response.json::<Value>().await.unwrap(), expected, "{ffqn}");
    server.shutdown().await;
}

#[tokio::test]
async fn add() {
    let deployment_toml = r#"[[activity_exec]]
ffqn = "testing:integration/exec-add.add"
content = '''#!/usr/bin/env bash
echo $(( $1 + $2 ))
'''
params = [
  { name = "a", type = "u32" },
  { name = "b", type = "u32" },
]
return_type = "result<u32, string>"
"#;
    activity_exec_case(
        test_addr!(50),
        deployment_toml,
        &[],
        "testing:integration/exec-add.add",
        vec![json!(3), json!(5)],
        json!({ "ok": 8 }),
    )
    .await;
}

// `greet` is served two ways: `greet-include` from a script file referenced via `location`,
// `greet-inline` from `content` embedded in the manifest. Both must produce the same result.
const GREET_SCRIPT: &str = r#"#!/usr/bin/env bash
set -exuo pipefail
raw=$(echo $1 | jq -r .)
echo "\"Hello, $raw!\""
"#;

#[tokio::test]
async fn greet_inline() {
    let deployment_toml = format!(
        r#"[[activity_exec]]
ffqn = "testing:integration/exec-greet.greet-inline"
content = '''
{GREET_SCRIPT}'''
params = [
  {{ name = "name", type = "string" }},
]
return_type = "result<string, string>"
env_vars = ["PATH"] # for jq
"#
    );
    activity_exec_case(
        test_addr!(52),
        &deployment_toml,
        &[],
        "testing:integration/exec-greet.greet-inline",
        vec![json!("World")],
        json!({ "ok": "Hello, World!" }),
    )
    .await;
}

#[tokio::test]
async fn greet_include() {
    let deployment_toml = r#"[[activity_exec]]
ffqn = "testing:integration/exec-greet.greet-include"
location = "./greet.sh"
params = [
  { name = "name", type = "string" },
]
return_type = "result<string, string>"
env_vars = ["PATH"] # for jq
"#;
    activity_exec_case(
        test_addr!(53),
        deployment_toml,
        &[("greet.sh", GREET_SCRIPT)],
        "testing:integration/exec-greet.greet-include",
        vec![json!("World")],
        json!({ "ok": "Hello, World!" }),
    )
    .await;
}

#[tokio::test]
async fn record_return_type() {
    let deployment_toml = r#"[[activity_exec]]
ffqn = "testing:integration/exec-record.make-record"
content = '''#!/usr/bin/env bash
printf '{"name": "Alice", "count": 42}'
'''
return_type = "result<record { name: string, count: u32 }, string>"
"#;
    activity_exec_case(
        test_addr!(54),
        deployment_toml,
        &[],
        "testing:integration/exec-record.make-record",
        vec![],
        json!({ "ok": { "name": "Alice", "count": 42 } }),
    )
    .await;
}

#[tokio::test]
async fn stdin_secrets() {
    let deployment_toml = r#"[[activity_exec]]
ffqn = "testing:integration/exec-stdin.expose-secrets"
content = '''#!/usr/bin/env bash
set -exuo pipefail
jq -R . /dev/stdin
'''
return_type = "result<string, string>"
env_vars = ["PATH"] # for jq
secrets = ["MY_SECRET"]
"#;
    let server = TestServer::start_inline_deployment(test_addr!(55), "", deployment_toml, &[]).await;
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
async fn void_ok() {
    let deployment_toml = r#"[[activity_exec]]
content = '''#!/bin/sh
true
'''
ffqn = "testing:integration/exec-void.void-ok"
return_type = "result"
"#;
    activity_exec_case(
        test_addr!(56),
        deployment_toml,
        &[],
        "testing:integration/exec-void.void-ok",
        vec![],
        json!({ "ok": null }),
    )
    .await;
}

#[tokio::test]
async fn void_err() {
    let deployment_toml = r#"[[activity_exec]]
content = '''#!/bin/sh
false
'''
ffqn = "testing:integration/exec-void.void-err"
"#;
    activity_exec_case(
        test_addr!(57),
        deployment_toml,
        &[],
        "testing:integration/exec-void.void-err",
        vec![],
        json!({ "err": null }),
    )
    .await;
}

#[tokio::test]
async fn args_passthrough() {
    let deployment_toml = r#"[[activity_exec]]
ffqn = "testing:integration/exec-args.echo-args"
content = '''#!/usr/bin/env bash
# Receives two u32 params as JSON args: $1 and $2
printf '{"a": %s, "b": %s}' "$1" "$2"
'''
params = [
  { name = "a", type = "u32" },
  { name = "b", type = "u32" },
]
return_type = "result<record { a: u32, b: u32 }, string>"
"#;
    activity_exec_case(
        test_addr!(58),
        deployment_toml,
        &[],
        "testing:integration/exec-args.echo-args",
        vec![json!(10), json!(20)],
        // The bash script receives JSON-serialized params as positional args ($1=10, $2=20)
        // and echoes them back as a JSON record: {"a": 10, "b": 20}
        json!({ "ok": { "a": 10, "b": 20 } }),
    )
    .await;
}

#[tokio::test]
async fn args_via_stdin() {
    let deployment_toml = r#"[[activity_exec]]
ffqn = "testing:integration/exec-stdin-args.echo-args"
content = '''#!/usr/bin/env bash
set -euo pipefail
# Receives params via the stdin JSON `params` array instead of argv.
jq -c '{a: .params[0], b: .params[1]}' /dev/stdin
'''
params = [
  { name = "a", type = "u32" },
  { name = "b", type = "u32" },
]
return_type = "result<record { a: u32, b: u32 }, string>"
env_vars = ["PATH"] # for jq
params_via_stdin = true
"#;
    activity_exec_case(
        test_addr!(82),
        deployment_toml,
        &[],
        "testing:integration/exec-stdin-args.echo-args",
        vec![json!(10), json!(20)],
        // With params_via_stdin, params arrive in the stdin JSON `params` array
        // (argv carries none); the script echoes them back as {"a": 10, "b": 20}.
        json!({ "ok": { "a": 10, "b": 20 } }),
    )
    .await;
}

#[tokio::test]
async fn env_vars() {
    let deployment_toml = r#"[[activity_exec]]
ffqn = "testing:integration/exec-env.read-env"
content = '''#!/usr/bin/env bash
echo \"$MY_VAR\"
'''
return_type = "result<string, string>"
env_vars = [{key = "MY_VAR", value = "hello_from_exec_env"}]
"#;
    activity_exec_case(
        test_addr!(59),
        deployment_toml,
        &[],
        "testing:integration/exec-env.read-env",
        vec![],
        json!({ "ok": "hello_from_exec_env" }),
    )
    .await;
}

#[tokio::test]
async fn error_exit() {
    let deployment_toml = r#"[[activity_exec]]
ffqn = "testing:integration/exec-error.fail"
content = '''#!/usr/bin/env bash
echo '"something went wrong"'
exit 1
'''
return_type = "result<string, string>"
"#;
    activity_exec_case(
        test_addr!(60),
        deployment_toml,
        &[],
        "testing:integration/exec-error.fail",
        vec![],
        json!({ "err": "something went wrong" }),
    )
    .await;
}

#[tokio::test]
async fn stream_logs() {
    let deployment_toml = r#"[[activity_exec]]
ffqn = "testing:integration/exec-stream.stream-test"
content = '''#!/usr/bin/env bash
echo "line1" >&2
sleep 0.1
echo "line2" >&2
'''
env_vars = ["PATH"] # for sleep
"#;
    let server = TestServer::start_inline_deployment(test_addr!(61), "", deployment_toml, &[]).await;
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

    let stderr_entries = loop {
        let logs = server.get_logs(&exec_id, 2).await;
        debug!("Fetched logs: {logs:?}");
        let stderr_entries: Vec<Value> = logs
            .as_array()
            .expect("logs must be an array")
            .iter()
            .filter(|entry| entry["type"] == "stream" && entry["stream_type"] == "stderr")
            .cloned()
            .collect();
        if stderr_entries.len() == 2 {
            break stderr_entries;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };

    // Streaming must produce 2 separate stderr entries (one per echo).
    assert_eq!(
        2,
        stderr_entries.len(),
        "expected 2 stderr stream entries, got {}: {stderr_entries:?}",
        stderr_entries.len(),
    );
    server.shutdown().await;
}
