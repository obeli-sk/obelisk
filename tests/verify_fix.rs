use std::process::Command;

#[test]
fn verify_fix_uses_http_servers_from_server_config() {
    let tmp = tempfile::tempdir().unwrap();
    let server_path = tmp.path().join("server.toml");
    let app_path = tmp.path().join("app.toml");
    let deployment_path = tmp.path().join("deployment.toml");
    let app = "app_name = \"verify-fix-http-servers\"\n";
    let declared_app = format!("{app}\n[public_env]\nOBELISK_VERIFY_FIX_TEST_ENV = {{}}\n");
    let named_webhook = r#"
[[webhook_endpoint_js]]
name = "hello"
http_server = "api"
env_vars = ["OBELISK_VERIFY_FIX_TEST_ENV"]
routes = ["/hello"]
content = '''
export default function handle() {
    return new Response("hello");
}
'''
"#;
    let external_webhook = r#"
[[webhook_endpoint_js]]
name = "external_hello"
http_server = "external"
routes = ["/hello"]
content = '''
export default function handle() {
    return new Response("hello");
}
'''
"#;

    for external_enabled in [true, false] {
        std::fs::write(
            &server_path,
            format!(
                r#"webui.enabled = false
external.enabled = {external_enabled}
external.listening_addr = "127.0.0.1:19090"

[database.sqlite]
directory = "db"

[wasm]
cache_directory = "wasm-cache"

[wasm.codegen_cache]
directory = "codegen-cache"

[[http_server]]
name = "api"
listening_addr = "127.0.0.1:19091"
"#
            ),
        )
        .unwrap();
        let deployment = if external_enabled {
            format!("{named_webhook}{external_webhook}")
        } else {
            named_webhook.to_owned()
        };

        for command in ["deployment", "server"] {
            for fix in [false, true] {
                std::fs::write(&app_path, if fix { app } else { &declared_app }).unwrap();
                let mut manifest = deployment.parse::<toml_edit::DocumentMut>().unwrap();
                if fix {
                    manifest["webhook_endpoint_js"][0]["content_digest"] = toml_edit::value(
                        "sha256:0000000000000000000000000000000000000000000000000000000000000000",
                    );
                }
                std::fs::write(&deployment_path, manifest.to_string()).unwrap();

                let mut cli = Command::new(env!("CARGO_BIN_EXE_obelisk"));
                cli.args([command, "verify"])
                    .arg("--server-config")
                    .arg(&server_path)
                    .arg("--app-config")
                    .arg(&app_path)
                    .arg("--deployment")
                    .arg(&deployment_path)
                    .env("OBELISK_JS_RUNTIME", "v8")
                    .env("OBELISK_VERIFY_FIX_TEST_ENV", "hello")
                    .env_remove("OBELISK_UNSTABLE_ACTIVITY_VM");
                if fix {
                    cli.arg("--fix");
                }
                let output = cli.output().unwrap();
                assert!(
                    output.status.success(),
                    "{command} verify (fix={fix}, external.enabled={external_enabled}) failed:\n{}\n{}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr),
                );
                if fix {
                    let fixed_app = std::fs::read_to_string(&app_path)
                        .unwrap()
                        .parse::<toml_edit::DocumentMut>()
                        .unwrap();
                    assert!(
                        fixed_app["public_env"]
                            .as_table()
                            .unwrap()
                            .contains_key("OBELISK_VERIFY_FIX_TEST_ENV")
                    );
                    let fixed_deployment = std::fs::read_to_string(&deployment_path)
                        .unwrap()
                        .parse::<toml_edit::DocumentMut>()
                        .unwrap();
                    assert_ne!(
                        fixed_deployment["webhook_endpoint_js"][0]["content_digest"].as_str(),
                        manifest["webhook_endpoint_js"][0]["content_digest"].as_str(),
                    );
                }
            }
        }
    }
}
