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

        {
            let command = "server";
            for fix in [false, true] {
                std::fs::write(&app_path, if fix { app } else { &declared_app }).unwrap();
                std::fs::write(&deployment_path, &deployment).unwrap();

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
                }
                assert_eq!(
                    std::fs::read_to_string(&deployment_path).unwrap(),
                    deployment
                );
            }
        }
    }
}

struct RepairFixture {
    dir: tempfile::TempDir,
}

impl RepairFixture {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("server.toml"),
            "allowed_exec_activities = false\nwebui.enabled = false\n",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("app.toml"),
            "app_name = 'repair-flags'\n[secrets]\nTOKEN = {}\n",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("deployment.toml"),
            r##"
[[activity_exec]]
name = "worker"
ffqn = "test:repair/worker.run"
content = "#!/bin/sh\necho null\n"
env_vars = ["OBELISK_VERIFY_REPAIR_MISSING"]
exposed_secrets = ["TOKEN"]

[[activity_js]]
name = "http"
ffqn = "test:repair/http.run"
content = "export default () => null;"
[[activity_js.allowed_host]]
pattern = "https://api.example.com"
methods = ["GET"]
secrets = ["TOKEN"]
replace_in = ["headers"]
"##,
        )
        .unwrap();
        Self { dir }
    }

    fn run(&self, flags: &[&str]) -> std::process::Output {
        Command::new(env!("CARGO_BIN_EXE_obelisk"))
            .args([
                "server",
                "verify",
                "--skip-db",
                "-s",
                "server.toml",
                "-a",
                "app.toml",
                "-d",
                "deployment.toml",
                "--allow-unavailable-runtime-config",
            ])
            .args(flags)
            .current_dir(self.dir.path())
            .env("OBELISK_JS_RUNTIME", "v8")
            .env_remove("OBELISK_VERIFY_REPAIR_MISSING")
            .env_remove("TOKEN")
            .env_remove("OBELISK_UNSTABLE_ACTIVITY_VM")
            .output()
            .unwrap()
    }

    fn read(&self, name: &str) -> String {
        std::fs::read_to_string(self.dir.path().join(name)).unwrap()
    }

    fn app(&self) -> toml::Value {
        toml::from_str(&self.read("app.toml")).unwrap()
    }
}

#[test]
fn verification_repairs_are_scoped_and_platform_grants_are_explicit() {
    for flag in [
        "--fix-app-declarations",
        "--fix-app-http-permissions",
        "--fix-app-exec-permissions",
        "--fix-app-secret-exposure",
        "--fix-server-exec-permissions",
        "--fix",
    ] {
        let fixture = RepairFixture::new();
        let original_server = fixture.read("server.toml");
        let deployment = fixture.read("deployment.toml");
        let output = fixture.run(&[flag]);
        assert!(
            !output.status.success(),
            "one repair must not grant the other missing permissions"
        );
        let app = fixture.app();
        let app_all = flag == "--fix";
        assert_eq!(
            app.get("public_env").is_some(),
            app_all || flag == "--fix-app-declarations",
            "{flag}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            app.get("outbound_http").is_some(),
            app_all || flag == "--fix-app-http-permissions",
            "{flag}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            app.get("allowed_exec_activities").is_some(),
            app_all || flag == "--fix-app-exec-permissions",
            "{flag}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            app["secrets"]["TOKEN"].get("exposed_to").is_some(),
            app_all || flag == "--fix-app-secret-exposure",
            "{flag}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        if flag == "--fix-server-exec-permissions" {
            let server: toml::Value = toml::from_str(&fixture.read("server.toml")).unwrap();
            assert!(
                server["allowed_exec_activities"]["worker"]
                    .as_str()
                    .unwrap()
                    .starts_with("sha256:")
            );
        } else {
            assert_eq!(fixture.read("server.toml"), original_server, "{flag}");
        }
        assert_eq!(fixture.read("deployment.toml"), deployment);
    }
}

#[test]
fn app_and_platform_repairs_verify_and_are_idempotent() {
    let fixture = RepairFixture::new();
    let old_digest = "sha256:1111111111111111111111111111111111111111111111111111111111111111";
    std::fs::write(
        fixture.dir.path().join("server.toml"),
        format!(
            "allowed_exec_activities = {{ unrelated = '{old_digest}' }}\nwebui.enabled = false\n"
        ),
    )
    .unwrap();
    let flags = ["--fix", "--fix-server-exec-permissions"];
    let output = fixture.run(&flags);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let app = fixture.read("app.toml");
    let server = fixture.read("server.toml");
    let policy: toml::Value = toml::from_str(&server).unwrap();
    assert_eq!(
        policy["allowed_exec_activities"]["unrelated"].as_str(),
        Some(old_digest)
    );
    let output = fixture.run(&flags);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(fixture.read("app.toml"), app);
    assert_eq!(fixture.read("server.toml"), server);
    let output = fixture.run(&[]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );

    let mut policy = fixture.app();
    policy
        .as_table_mut()
        .unwrap()
        .remove("allowed_exec_activities");
    std::fs::write(
        fixture.dir.path().join("app.toml"),
        toml::to_string(&policy).unwrap(),
    )
    .unwrap();
    let output = fixture.run(&[]);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("not authorized"));
}

#[test]
fn server_value_tolerance_still_enforces_http_and_secret_permissions() {
    let fixture = RepairFixture::new();
    let output = fixture.run(&["--fix", "--fix-server-exec-permissions"]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let approved = fixture.read("app.toml");
    let mut policy: toml::Value = toml::from_str(&approved).unwrap();
    policy.as_table_mut().unwrap().remove("outbound_http");
    std::fs::write(
        fixture.dir.path().join("app.toml"),
        toml::to_string(&policy).unwrap(),
    )
    .unwrap();
    let output = fixture.run(&[]);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("outbound HTTP"));

    let mut policy: toml::Value = toml::from_str(&approved).unwrap();
    policy["secrets"]["TOKEN"]
        .as_table_mut()
        .unwrap()
        .remove("exposed_to");
    std::fs::write(
        fixture.dir.path().join("app.toml"),
        toml::to_string(&policy).unwrap(),
    )
    .unwrap();
    let output = fixture.run(&[]);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("not exposed"));
}

#[test]
fn deployment_verification_ignores_policy_files_and_runtime_values() {
    let fixture = RepairFixture::new();
    std::fs::write(fixture.dir.path().join("app.toml"), "invalid TOML [").unwrap();
    std::fs::write(fixture.dir.path().join("server.toml"), "invalid TOML [").unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_obelisk"))
        .args(["deployment", "verify", "-d", "deployment.toml"])
        .current_dir(fixture.dir.path())
        .env("OBELISK_JS_RUNTIME", "v8")
        .env_remove("OBELISK_APP_NAME")
        .env_remove("OBELISK_VERIFY_REPAIR_MISSING")
        .env_remove("TOKEN")
        .env_remove("OBELISK_UNSTABLE_ACTIVITY_VM")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}
